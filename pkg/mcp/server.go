// Package mcp provides the JSON-RPC server for module coordination protocol.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/dkooll/wamcp/internal/database"
	"github.com/dkooll/wamcp/internal/formatter"
	"github.com/dkooll/wamcp/internal/indexer"
	"github.com/dkooll/wamcp/internal/util"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

type Message struct {
	JSONRPC string    `json:"jsonrpc"`
	Method  string    `json:"method,omitempty"`
	Params  any       `json:"params,omitempty"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ToolCallParams struct {
	Name      string `json:"name"`
	Arguments any    `json:"arguments"`
}

var errModuleNotInPrompt = errors.New("module not found in prompt")

type Server struct {
	db        *database.DB
	syncer    *indexer.Syncer
	writer    io.Writer
	jobs      map[string]*SyncJob
	jobsMutex sync.RWMutex
	dbPath    string
	token     string
	org       string
	dbMutex   sync.Mutex
}

func NewServer(dbPath, token, org string) *Server {
	return &Server{
		dbPath: dbPath,
		token:  token,
		org:    org,
		jobs:   make(map[string]*SyncJob),
	}
}

type SyncJob struct {
	ID          string
	Type        string
	Status      string
	StartedAt   time.Time
	CompletedAt *time.Time
	Progress    *indexer.SyncProgress
	Error       string
}

func (s *Server) ensureDB() error {
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	if s.db != nil {
		return nil
	}

	log.Printf("Initializing database at: %s", s.dbPath)
	db, err := database.New(s.dbPath)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	s.db = db
	s.syncer = indexer.NewSyncer(db, s.token, s.org)
	log.Println("Database initialized successfully")

	return nil
}

func (s *Server) Run(ctx context.Context, r io.Reader, w io.Writer) error {
	s.writer = w
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		log.Printf("Received: %s", line)

		var msg Message
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			log.Printf("Failed to parse message: %v", err)
			s.sendError(-32700, "Parse error", nil)
			continue
		}

		s.handleMessage(msg)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	return nil
}

func (s *Server) handleMessage(msg Message) {
	log.Printf("Handling method: %s", msg.Method)

	switch msg.Method {
	case "initialize":
		s.handleInitialize(msg)
	case "initialized":
		log.Println("Client initialized")
	case "tools/list":
		s.handleToolsList(msg)
	case "tools/call":
		s.handleToolsCall(msg)
	case "notifications/cancelled":
		log.Println("Request cancelled")
	default:
		s.sendError(-32601, "Method not found", msg.ID)
	}
}

func (s *Server) handleInitialize(msg Message) {
	response := Message{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result: map[string]any{
			"protocolVersion": "2024-11-05",
			"serverInfo": map[string]any{
				"name":    "az-cn-wam",
				"version": "1.0.0",
			},
			"capabilities": map[string]any{
				"tools": map[string]any{},
			},
		},
	}
	s.sendResponse(response)
}

func (s *Server) handleToolsList(msg Message) {
	tools := []map[string]any{
		{
			"name":        "sync_modules",
			"description": "Sync all Terraform modules from GitHub to local database",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "sync_updates_modules",
			"description": "Incrementally sync only updated Terraform modules from GitHub (skips unchanged modules)",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "list_modules",
			"description": "List all available Terraform modules from local database",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "search_modules",
			"description": "Search modules by name or description in local database",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{
						"type":        "string",
						"description": "Search query",
					},
					"limit": map[string]any{
						"type":        "number",
						"description": "Maximum number of results (default: 10)",
					},
				},
				"required": []string{"query"},
			},
		},
		{
			"name":        "get_module_info",
			"description": "Get detailed information about a specific module including all files, variables, outputs, resources",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"module_name": map[string]any{
						"type":        "string",
						"description": "Name of the module",
					},
				},
				"required": []string{"module_name"},
			},
		},
		{
			"name":        "search_code",
			"description": "Search across all Terraform code files for specific patterns or text",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{
						"type":        "string",
						"description": "Text or pattern to search for in code",
					},
					"limit": map[string]any{
						"type":        "number",
						"description": "Maximum number of results (default: 20)",
					},
					"kind": map[string]any{
						"type":        "string",
						"description": "Optional structural filter: resource|dynamic|lifecycle",
					},
					"type_prefix": map[string]any{
						"type":        "string",
						"description": "Optional resource type prefix (e.g., azurerm_storage_account)",
					},
					"has": map[string]any{
						"type":        "array",
						"items":       map[string]any{"type": "string"},
						"description": "Optional attribute presence filters (e.g., for_each, lifecycle.ignore_changes)",
					},
				},
				"required": []string{"query"},
			},
		},
		{
			"name":        "get_file_content",
			"description": "Get the full content of a specific file from a module (e.g., variables.tf, main.tf, outputs.tf)",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"module_name": map[string]any{
						"type":        "string",
						"description": "Name of the module (e.g., terraform-azure-aks)",
					},
					"file_path": map[string]any{
						"type":        "string",
						"description": "Path to the file within the module (e.g., variables.tf, main.tf, README.md)",
					},
				},
				"required": []string{"module_name", "file_path"},
			},
		},
		{
			"name":        "extract_variable_definition",
			"description": "Extract the complete definition of a specific variable from a module's variables.tf",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"module_name": map[string]any{
						"type":        "string",
						"description": "Name of the module (e.g., terraform-azure-aks)",
					},
					"variable_name": map[string]any{
						"type":        "string",
						"description": "Name of the variable (e.g., cluster, config, instance)",
					},
				},
				"required": []string{"module_name", "variable_name"},
			},
		},
		{
			"name":        "compare_pattern_across_modules",
			"description": "Compare a specific code pattern (e.g., dynamic blocks, resource definitions) across all modules to find differences. Returns a summary table by default, or full code blocks if requested.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"pattern": map[string]any{
						"type":        "string",
						"description": "The pattern to search for (e.g., 'dynamic \"identity\"', 'resource \"azurerm_', 'lifecycle {')",
					},
					"file_type": map[string]any{
						"type":        "string",
						"description": "Optional: filter by file type (e.g., 'main.tf', 'variables.tf'). Leave empty for all .tf files.",
					},
					"show_full_blocks": map[string]any{
						"type":        "boolean",
						"description": "Optional: show full code blocks instead of summary (default: false for compact table view)",
					},
					"limit": map[string]any{
						"type":        "number",
						"description": "Optional: maximum number of results to return (default: unlimited for table view, 20 for full blocks)",
					},
					"offset": map[string]any{
						"type":        "number",
						"description": "Optional: number of results to skip for pagination (default: 0)",
					},
				},
				"required": []string{"pattern"},
			},
		},
		{
			"name":        "analyze_code_relationships",
			"description": "Reveal how a term is referenced within a module by leveraging indexed Terraform relationships (variables, resources, data sources). Accepts structured arguments or a natural-language prompt like 'Show subnet usage in redis.'",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"module_name": map[string]any{
						"type":        "string",
						"description": "Name or alias of the module to inspect",
					},
					"query": map[string]any{
						"type":        "string",
						"description": "Term to match against attribute paths or reference names (e.g., 'subnet')",
					},
					"limit": map[string]any{
						"type":        "number",
						"description": "Optional: maximum number of relationships to return (default 20)",
					},
					"prompt": map[string]any{
						"type":        "string",
						"description": "Natural-language request (e.g., 'Show subnet relationships in redis, top 5').",
					},
				},
			},
		},
		{
			"name":        "list_module_examples",
			"description": "List all available usage examples for a specific module",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"module_name": map[string]any{
						"type":        "string",
						"description": "Name of the module (e.g., terraform-azure-aks)",
					},
				},
				"required": []string{"module_name"},
			},
		},
		{
			"name":        "get_example_content",
			"description": "Get the complete content of a specific example including all files (main.tf, variables.tf, etc.)",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"module_name": map[string]any{
						"type":        "string",
						"description": "Name of the module (e.g., terraform-azure-aks)",
					},
					"example_name": map[string]any{
						"type":        "string",
						"description": "Name of the example (e.g., 'default', 'complete')",
					},
				},
				"required": []string{"module_name", "example_name"},
			},
		},
		{
			"name":        "sync_status",
			"description": "Get status of ongoing or previous sync jobs",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"job_id": map[string]any{
						"type":        "string",
						"description": "Optional job identifier returned by sync commands",
					},
				},
			},
		},
	}

	response := Message{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result: map[string]any{
			"tools": tools,
		},
	}
	s.sendResponse(response)
}

func (s *Server) handleToolsCall(msg Message) {
	paramsBytes, err := json.Marshal(msg.Params)
	if err != nil {
		s.sendError(-32602, "Invalid params", msg.ID)
		return
	}

	var params ToolCallParams
	if err := json.Unmarshal(paramsBytes, &params); err != nil {
		s.sendError(-32602, "Invalid params", msg.ID)
		return
	}

	log.Printf("Tool call: %s", params.Name)

	var result any
	switch params.Name {
	case "sync_modules":
		result = s.handleSyncModules()
	case "sync_updates_modules":
		result = s.handleSyncUpdatesModules()
	case "list_modules":
		result = s.handleListModules()
	case "search_modules":
		result = s.handleSearchModules(params.Arguments)
	case "get_module_info":
		result = s.handleGetModuleInfo(params.Arguments)
	case "search_code":
		result = s.handleSearchCode(params.Arguments)
	case "get_file_content":
		result = s.handleGetFileContent(params.Arguments)
	case "extract_variable_definition":
		result = s.handleExtractVariableDefinition(params.Arguments)
	case "compare_pattern_across_modules":
		result = s.handleComparePatternAcrossModules(params.Arguments)
	case "analyze_code_relationships":
		result = s.handleAnalyzeCodeRelationships(params.Arguments)
	case "list_module_examples":
		result = s.handleListModuleExamples(params.Arguments)
	case "get_example_content":
		result = s.handleGetExampleContent(params.Arguments)
	case "sync_status":
		result = s.handleSyncStatus(params.Arguments)
	default:
		s.sendError(-32601, "Tool not found", msg.ID)
		return
	}

	response := Message{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  result,
	}
	s.sendResponse(response)
}

func (s *Server) handleSyncModules() map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	job := s.startSyncJob("full_sync", func() (*indexer.SyncProgress, error) {
		log.Println("Starting full repository sync (async job)...")
		return s.syncer.SyncAll()
	})

	return map[string]any{
		"content": []map[string]any{
			{
				"type": "text",
				"text": fmt.Sprintf("Full sync started.\nJob ID: %s\nUse `sync_status` with this job ID to monitor progress.", job.ID),
			},
		},
	}
}

func (s *Server) handleSyncUpdatesModules() map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	log.Println("Starting incremental repository sync (updates only)...")

	progress, err := s.syncer.SyncUpdates()
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Sync failed: %v", err))
	}

	text := formatter.IncrementalSyncProgress(
		progress.TotalRepos,
		len(progress.UpdatedRepos),
		progress.SkippedRepos,
		progress.UpdatedRepos,
		progress.Errors,
	)

	return SuccessResponse(text)
}

func (s *Server) handleSyncStatus(args any) map[string]any {
	statusArgs, err := UnmarshalArgs[struct {
		JobID string `json:"job_id"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid parameters")
	}

	if statusArgs.JobID != "" {
		job, ok := s.getJob(statusArgs.JobID)
		if !ok {
			return ErrorResponse(fmt.Sprintf("Job '%s' not found", statusArgs.JobID))
		}

		text := s.formatJobDetails(job)
		return SuccessResponse(text)
	}

	jobs := s.listJobs()
	text := s.formatJobList(jobs)
	return SuccessResponse(text)
}

func (s *Server) handleListModules() map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	modules, err := s.db.ListModules()
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Error loading modules: %v", err))
	}

	if len(modules) == 0 {
		return SuccessResponse("No modules found. Run sync_modules tool to fetch modules from GitHub.")
	}

	text := formatter.ModuleList(modules)
	return SuccessResponse(text)
}

func (s *Server) handleSearchModules(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	searchArgs, err := UnmarshalArgs[struct {
		Query string `json:"query"`
		Limit int    `json:"limit"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid search query")
	}

	if searchArgs.Limit == 0 {
		searchArgs.Limit = 10
	}

	variants := util.ExpandQueryVariants(searchArgs.Query)
	seen := make(map[int64]struct{})
	var merged []database.Module
	for _, v := range variants {
		mods, err := s.db.SearchModules(v, searchArgs.Limit)
		if err != nil {
			continue
		}
		for _, m := range mods {
			if _, ok := seen[m.ID]; ok {
				continue
			}
			seen[m.ID] = struct{}{}
			merged = append(merged, m)
			if searchArgs.Limit > 0 && len(merged) >= searchArgs.Limit {
				break
			}
		}
		if searchArgs.Limit > 0 && len(merged) >= searchArgs.Limit {
			break
		}
	}

	text := formatter.SearchResults(searchArgs.Query, merged)
	return SuccessResponse(text)
}

func (s *Server) handleGetModuleInfo(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	moduleArgs, err := UnmarshalArgs[struct {
		ModuleName string `json:"module_name"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid module name")
	}

	module, err := s.resolveModule(moduleArgs.ModuleName)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Module '%s' not found", moduleArgs.ModuleName))
	}

	variables, _ := s.db.GetModuleVariables(module.ID)
	outputs, _ := s.db.GetModuleOutputs(module.ID)
	resources, _ := s.db.GetModuleResources(module.ID)
	files, _ := s.db.GetModuleFiles(module.ID)

	summary, _ := s.db.SummarizeModuleStructure(module.ID)
	text := formatter.ModuleInfo(module, variables, outputs, resources, files)
	if summary != nil {
		text += formatter.StructuralSummaryValues(summary.ResourceCount, summary.LifecycleCount, summary.ResourcesWithIgnoreChanges, summary.TopResourceTypes, summary.DynamicLabels)
	}
	return SuccessResponse(text)
}

func (s *Server) handleSearchCode(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	searchArgs, err := UnmarshalArgs[struct {
		Query      string   `json:"query"`
		Limit      int      `json:"limit"`
		Kind       string   `json:"kind"`
		TypePrefix string   `json:"type_prefix"`
		Has        []string `json:"has"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid search query")
	}

	if searchArgs.Limit == 0 {
		searchArgs.Limit = 20
	}

	variants := util.ExpandQueryVariants(searchArgs.Query)
	if len(variants) == 0 {
		variants = []string{searchArgs.Query}
	}

	seen := make(map[int64]struct{})
	var merged []database.ModuleFile
	var files []database.ModuleFile
	if len(variants) == 1 {
		files, _ = s.db.SearchFiles(variants[0], searchArgs.Limit)
	} else {
		parts := make([]string, 0, len(variants))
		for _, v := range variants {
			escaped := strings.ReplaceAll(v, "\"", "\"\"")
			parts = append(parts, fmt.Sprintf("\"%s\"", escaped))
		}
		match := strings.Join(parts, " OR ")
		files, _ = s.db.SearchFilesFTS(match, searchArgs.Limit)
	}

	for _, f := range files {
		if _, ok := seen[f.ID]; ok {
			continue
		}
		if searchArgs.Kind != "" || searchArgs.TypePrefix != "" || len(searchArgs.Has) > 0 {
			mod, merr := s.db.GetModuleByID(f.ModuleID)
			if merr != nil {
				continue
			}
			okStruct, herr := s.db.HCLBlockExists(mod.ID, f.FilePath, searchArgs.Kind, searchArgs.TypePrefix, searchArgs.Has)
			if herr != nil || !okStruct {
				continue
			}
		}
		seen[f.ID] = struct{}{}
		merged = append(merged, f)
		if searchArgs.Limit > 0 && len(merged) >= searchArgs.Limit {
			break
		}
	}

	getModuleName := func(moduleID int64) string {
		module, err := s.db.GetModuleByID(moduleID)
		if err == nil {
			return module.Name
		}
		return "unknown"
	}

	text := formatter.CodeSearchResults(searchArgs.Query, merged, getModuleName)
	return SuccessResponse(text)
}

func (s *Server) handleGetFileContent(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	fileArgs, err := UnmarshalArgs[struct {
		ModuleName string `json:"module_name"`
		FilePath   string `json:"file_path"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid parameters")
	}

	module, err := s.resolveModule(fileArgs.ModuleName)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Module '%s' not found", fileArgs.ModuleName))
	}
	file, err := s.db.GetFile(module.Name, fileArgs.FilePath)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("File '%s' not found in module '%s'", fileArgs.FilePath, module.Name))
	}

	text := formatter.FileContent(module.Name, file.FilePath, file.FileType, file.SizeBytes, file.Content)
	return SuccessResponse(text)
}

func (s *Server) handleExtractVariableDefinition(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	varArgs, err := UnmarshalArgs[struct {
		ModuleName   string `json:"module_name"`
		VariableName string `json:"variable_name"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid parameters")
	}

	module, err := s.resolveModule(varArgs.ModuleName)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Module '%s' not found", varArgs.ModuleName))
	}
	file, err := s.db.GetFile(module.Name, "variables.tf")
	if err != nil {
		return ErrorResponse(fmt.Sprintf("variables.tf not found in module '%s'", module.Name))
	}

	variableBlock := extractVariableBlock(file.Content, varArgs.VariableName)
	if variableBlock == "" {
		return ErrorResponse(fmt.Sprintf("Variable '%s' not found in %s", varArgs.VariableName, varArgs.ModuleName))
	}

	text := formatter.VariableDefinition(module.Name, varArgs.VariableName, variableBlock)
	return SuccessResponse(text)
}

func extractVariableBlock(content, variableName string) string {
	variablePattern := fmt.Sprintf(`variable "%s"`, variableName)
	startIdx := strings.Index(content, variablePattern)
	if startIdx == -1 {
		return ""
	}

	braceCount := 0
	inBlock := false
	endIdx := startIdx

Loop:
	for i := startIdx; i < len(content); i++ {
		char := content[i]
		switch char {
		case '{':
			braceCount++
			inBlock = true
		case '}':
			braceCount--
			if inBlock && braceCount == 0 {
				endIdx = i + 1
				break Loop
			}
		}
	}

	return content[startIdx:endIdx]
}

func (s *Server) handleComparePatternAcrossModules(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	patternArgs, err := UnmarshalArgs[struct {
		Pattern        string `json:"pattern"`
		FileType       string `json:"file_type"`
		ShowFullBlocks bool   `json:"show_full_blocks"`
		Limit          int    `json:"limit"`
		Offset         int    `json:"offset"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid parameters")
	}

	if patternArgs.Limit == 0 && patternArgs.ShowFullBlocks {
		patternArgs.Limit = 20
	}

	modules, err := s.db.ListModules()
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Error loading modules: %v", err))
	}

	results := s.findPatternMatches(modules, patternArgs.Pattern, patternArgs.FileType)
	paginatedResults := paginateResults(results, patternArgs.Offset, patternArgs.Limit)

	text := formatter.PatternComparison(
		patternArgs.Pattern,
		paginatedResults,
		patternArgs.ShowFullBlocks,
		patternArgs.Offset,
		patternArgs.Limit,
		len(results),
	)

	return SuccessResponse(text)
}

func (s *Server) handleAnalyzeCodeRelationships(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	moduleName, query, limit, prompt, err := parseRelationshipArgs(args)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Error: %v", err))
	}

	moduleName = strings.TrimSpace(moduleName)
	query = strings.TrimSpace(query)
	prompt = strings.TrimSpace(prompt)

	var module *database.Module

	if prompt != "" {
		parsedModule, parsedQuery, parsedLimit, parseErr := s.interpretRelationshipPrompt(prompt)
		if parseErr != nil {
			return ErrorResponse(fmt.Sprintf("Could not interpret prompt: %v", parseErr))
		}
		if moduleName == "" && parsedModule != nil {
			module = parsedModule
		}
		if query == "" {
			query = parsedQuery
		}
		if limit == 0 && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	if module == nil && moduleName != "" {
		module, err = s.resolveModule(moduleName)
		if err != nil {
			return ErrorResponse(fmt.Sprintf("Module '%s' not found", moduleName))
		}
	}

	if query == "" {
		return ErrorResponse("Error: query missing. Provide `query` or specify what you are looking for in the prompt.")
	}

	return s.runRelationshipQuery(module, query, limit)
}

func (s *Server) runRelationshipQuery(module *database.Module, query string, limit int) map[string]any {
	if module != nil {
		rels, err := s.db.QueryRelationships(module.ID, query, limit)
		if err != nil {
			return ErrorResponse(fmt.Sprintf("Failed to load relationships: %v", err))
		}

		if len(rels) == 0 {
			return SuccessResponse(fmt.Sprintf("No relationships matching '%s' found in module '%s'.", query, module.Name))
		}

		files, err := s.db.GetModuleFiles(module.ID)
		if err != nil {
			return ErrorResponse(fmt.Sprintf("Failed to load module files: %v", err))
		}

		fileMap := make(map[string]database.ModuleFile, len(files))
		for _, file := range files {
			fileMap[file.FilePath] = file
		}

		text := formatter.RelationshipAnalysis(module.Name, query, rels, fileMap)
		if limit > 0 && len(rels) == limit {
			text += fmt.Sprintf("\n_Note: Showing the first %d matches. Increase `limit` to see more._\n", limit)
		}

		return SuccessResponse(text)
	}

	rels, err := s.db.QueryRelationshipsAny(query, limit)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to load relationships: %v", err))
	}

	if len(rels) == 0 {
		return SuccessResponse(fmt.Sprintf("No relationships matching '%s' found across modules.", query))
	}

	buckets := make(map[int64][]database.HCLRelationship)
	for _, rel := range rels {
		buckets[rel.ModuleID] = append(buckets[rel.ModuleID], rel)
	}

	views := make([]formatter.ModuleRelationshipView, 0, len(buckets))
	for moduleID, items := range buckets {
		mod, err := s.db.GetModuleByID(moduleID)
		if err != nil {
			log.Printf("Warning: failed to load module %d for relationships: %v", moduleID, err)
			continue
		}

		files, err := s.db.GetModuleFiles(moduleID)
		if err != nil {
			log.Printf("Warning: failed to load files for module %s: %v", mod.Name, err)
			continue
		}

		fileMap := make(map[string]database.ModuleFile, len(files))
		for _, file := range files {
			fileMap[file.FilePath] = file
		}

		views = append(views, formatter.ModuleRelationshipView{
			ModuleName:    mod.Name,
			Relationships: items,
			Files:         fileMap,
		})
	}

	if len(views) == 0 {
		return SuccessResponse(fmt.Sprintf("No relationships matching '%s' found across modules.", query))
	}

	text := formatter.RelationshipAnalysisAcross(query, views)
	if limit > 0 && len(rels) == limit {
		text += fmt.Sprintf("\n_Note: Showing the first %d matches overall. Increase `limit` to see more._\n", limit)
	}

	return SuccessResponse(text)
}

func parseRelationshipArgs(raw any) (moduleName, query string, limit int, prompt string, err error) {
	if raw == nil {
		return "", "", 0, "", nil
	}

	switch v := raw.(type) {
	case string:
		return "", "", 0, v, nil
	case map[string]any:
		if val, ok := v["module_name"]; ok {
			s, ok := val.(string)
			if !ok {
				return "", "", 0, "", fmt.Errorf("module_name must be a string")
			}
			moduleName = s
		}
		if val, ok := v["query"]; ok {
			s, ok := val.(string)
			if !ok {
				return "", "", 0, "", fmt.Errorf("query must be a string")
			}
			query = s
		}
		if val, ok := v["prompt"]; ok {
			s, ok := val.(string)
			if !ok {
				return "", "", 0, "", fmt.Errorf("prompt must be a string")
			}
			prompt = s
		}
		if val, ok := v["limit"]; ok {
			switch t := val.(type) {
			case float64:
				limit = int(t)
			case int:
				limit = t
			case json.Number:
				n, err := t.Int64()
				if err != nil {
					return "", "", 0, "", fmt.Errorf("limit must be numeric")
				}
				limit = int(n)
			case string:
				if strings.TrimSpace(t) == "" {
					limit = 0
					break
				}
				n, err := strconv.Atoi(t)
				if err != nil {
					return "", "", 0, "", fmt.Errorf("limit must be numeric")
				}
				limit = n
			default:
				return "", "", 0, "", fmt.Errorf("limit must be numeric")
			}
		}
		return
	default:
		// try JSON round-trip for other shapes (e.g., struct)
		bytes, marshalErr := json.Marshal(raw)
		if marshalErr != nil {
			return "", "", 0, "", fmt.Errorf("unsupported parameter format")
		}
		var tmp struct {
			ModuleName string `json:"module_name"`
			Query      string `json:"query"`
			Limit      int    `json:"limit"`
			Prompt     string `json:"prompt"`
		}
		if err := json.Unmarshal(bytes, &tmp); err != nil {
			return "", "", 0, "", fmt.Errorf("invalid parameters")
		}
		return tmp.ModuleName, tmp.Query, tmp.Limit, tmp.Prompt, nil
	}
}

func (s *Server) interpretRelationshipPrompt(prompt string) (*database.Module, string, int, error) {
	original := strings.TrimSpace(prompt)
	if original == "" {
		return nil, "", 0, fmt.Errorf("prompt is empty")
	}

	limit, cleaned := extractPromptLimit(original)
	tokens := tokenizePrompt(cleaned)
	if len(tokens) == 0 {
		return nil, "", limit, fmt.Errorf("could not find useful words")
	}

	module, moduleIdx, err := s.findModuleFromTokens(tokens)
	if err != nil && !errors.Is(err, errModuleNotInPrompt) {
		return nil, "", limit, err
	}

	query := deriveQueryFromTokens(tokens, moduleIdx)
	if query == "" {
		return module, "", limit, fmt.Errorf("could not identify what to search for")
	}

	return module, query, limit, nil
}

func extractPromptLimit(prompt string) (int, string) {
	limitRegex := regexp.MustCompile(`(?i)\b(?:top|first|limit)\s+(\d{1,3})\b`)
	limit := 0
	cleaned := limitRegex.ReplaceAllStringFunc(prompt, func(match string) string {
		parts := limitRegex.FindStringSubmatch(match)
		if len(parts) > 1 {
			if parsed, err := strconv.Atoi(parts[1]); err == nil && parsed > 0 {
				limit = parsed
			}
		}
		return ""
	})
	return limit, cleaned
}

type promptToken struct {
	Original string
	Lower    string
}

func tokenizePrompt(input string) []promptToken {
	splitFn := func(r rune) bool {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '/' || r == '_' || r == '-' {
			return false
		}
		return true
	}

	raw := strings.FieldsFunc(input, splitFn)
	tokens := make([]promptToken, 0, len(raw))
	for _, part := range raw {
		if part == "" {
			continue
		}
		tokens = append(tokens, promptToken{Original: part, Lower: strings.ToLower(part)})
	}
	return tokens
}

func (s *Server) findModuleFromTokens(tokens []promptToken) (*database.Module, []int, error) {
	if len(tokens) == 0 {
		return nil, nil, fmt.Errorf("no tokens available")
	}

	maxWindow := min(3, len(tokens))

	tried := make(map[string]struct{})

	for window := maxWindow; window >= 1; window-- {
		for start := len(tokens) - window; start >= 0; start-- {
			segment := tokens[start : start+window]
			forms := candidateModuleForms(segment)
			for _, candidate := range forms {
				if candidate == "" {
					continue
				}
				if _, seen := tried[candidate]; seen {
					continue
				}
				tried[candidate] = struct{}{}
				module, err := s.resolveModule(candidate)
				if err == nil {
					indices := make([]int, window)
					for i := 0; i < window; i++ {
						indices[i] = start + i
					}
					return module, indices, nil
				}
			}
		}
	}

	return nil, nil, errModuleNotInPrompt
}

func candidateModuleForms(tokens []promptToken) []string {
	parts := make([]string, len(tokens))
	for i, t := range tokens {
		parts[i] = t.Lower
	}

	set := make(map[string]struct{})
	push := func(s string) {
		s = strings.TrimSpace(s)
		if s != "" {
			set[s] = struct{}{}
		}
	}

	push(strings.Join(parts, " "))
	if len(parts) > 1 {
		push(strings.Join(parts, "-"))
		push(strings.Join(parts, "_"))
	}
	push(strings.Join(parts, ""))

	forms := make([]string, 0, len(set))
	for key := range set {
		forms = append(forms, key)
	}
	return forms
}

var relationshipStopwords = map[string]struct{}{
	"show":          {},
	"me":            {},
	"please":        {},
	"the":           {},
	"a":             {},
	"an":            {},
	"module":        {},
	"modules":       {},
	"relationship":  {},
	"relationships": {},
	"in":            {},
	"within":        {},
	"inside":        {},
	"for":           {},
	"of":            {},
	"on":            {},
	"about":         {},
	"across":        {},
	"with":          {},
	"to":            {},
	"and":           {},
	"all":           {},
	"any":           {},
	"find":          {},
	"list":          {},
	"see":           {},
	"need":          {},
	"want":          {},
	"how":           {},
	"do":            {},
	"does":          {},
	"display":       {},
	"get":           {},
	"showing":       {},
	"tell":          {},
	"explain":       {},
	"look":          {},
	"into":          {},
	"where":         {},
	"which":         {},
	"top":           {},
	"first":         {},
	"limit":         {},
	"results":       {},
	"matches":       {},
}

func deriveQueryFromTokens(tokens []promptToken, moduleIdx []int) string {
	indexSet := make(map[int]struct{}, len(moduleIdx))
	for _, idx := range moduleIdx {
		indexSet[idx] = struct{}{}
	}

	var focusTokens []promptToken
	if len(moduleIdx) > 0 {
		first := moduleIdx[0]
		if first > 0 {
			focusTokens = append(focusTokens, tokens[:first]...)
		} else if last := moduleIdx[len(moduleIdx)-1]; last+1 < len(tokens) {
			focusTokens = append(focusTokens, tokens[last+1:]...)
		}
	}

	if len(focusTokens) == 0 {
		for i, tok := range tokens {
			if _, isModule := indexSet[i]; isModule {
				continue
			}
			focusTokens = append(focusTokens, tok)
		}
	}

	var filtered []string
	for _, tok := range focusTokens {
		if _, stop := relationshipStopwords[tok.Lower]; stop {
			continue
		}
		if _, err := strconv.Atoi(tok.Lower); err == nil {
			continue
		}
		filtered = append(filtered, tok.Original)
	}

	if len(filtered) == 0 {
		for _, tok := range focusTokens {
			filtered = append(filtered, tok.Original)
		}
	}

	return strings.TrimSpace(strings.Join(filtered, " "))
}

func (s *Server) findPatternMatches(modules []database.Module, pattern, fileType string) []formatter.PatternMatch {
	var results []formatter.PatternMatch

	indexed := s.findPatternMatchesIndexed(pattern, fileType)
	if len(indexed) > 0 {
		return indexed
	}

	for _, module := range modules {
		files, err := s.db.GetModuleFiles(module.ID)
		if err != nil {
			continue
		}

		for _, file := range files {
			if fileType != "" && file.FileName != fileType {
				continue
			}

			if !strings.HasSuffix(file.FileName, ".tf") {
				continue
			}

			matches := extractASTPatternMatches(file.Content, pattern)
			if len(matches) == 0 {
				for _, m := range extractPatternMatches(file.Content, pattern) {
					matches = append(matches, astMatch{Code: m, BlockType: "", Summary: ""})
				}
			}
			for i, match := range matches {
				displayName := module.Name
				if len(matches) > 1 {
					displayName = fmt.Sprintf("%s #%d", module.Name, i+1)
				}
				results = append(results, formatter.PatternMatch{
					ModuleName: displayName,
					FileName:   file.FileName,
					Match:      match.Code,
					BlockType:  match.BlockType,
					Summary:    match.Summary,
				})
			}
		}
	}

	return results
}

func (s *Server) findPatternMatchesIndexed(pattern, fileType string) []formatter.PatternMatch {
	trimmed := strings.TrimSpace(pattern)
	if trimmed == "" {
		return nil
	}

	hasFilters := parseHasFilters(trimmed)
	var blocks []database.HCLBlock
	var err error
	blockType := ""
	typeLabel := ""
	prefix := false

	if want, ok := getQuotedArg(trimmed, "resource"); ok {
		blockType = "resource"
		typeLabel = want
		prefix = true
	} else if want, ok := getQuotedArg(trimmed, "dynamic"); ok {
		blockType = "dynamic"
		typeLabel = want
		prefix = false
	} else if strings.HasPrefix(trimmed, "lifecycle") {
		blockType = "lifecycle"
	} else {
		return nil
	}

	blocks, err = s.db.QueryHCLBlocks(blockType, typeLabel, prefix)
	if err != nil || len(blocks) == 0 {
		return nil
	}

	var results []formatter.PatternMatch
	for _, b := range blocks {
		if fileType != "" && !strings.HasSuffix(b.FilePath, "/"+fileType) && !strings.HasSuffix(b.FilePath, fileType) {
			continue
		}
		if len(hasFilters) > 0 {
			ok := true
			attrSet := make(map[string]struct{})
			if b.AttrPaths.Valid {
				rest := b.AttrPaths.String
				for {
					line, r, cut := strings.Cut(rest, "\n")
					if line != "" {
						attrSet[line] = struct{}{}
					}
					if !cut {
						break
					}
					rest = r
				}
			}
			for _, f := range hasFilters {
				if _, present := attrSet[f]; !present {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
		}

		module, merr := s.db.GetModuleByID(b.ModuleID)
		if merr != nil {
			continue
		}
		f, ferr := s.db.GetFile(module.Name, b.FilePath)
		if ferr != nil {
			continue
		}
		start := int(b.StartByte)
		end := int(b.EndByte)
		if start < 0 {
			start = 0
		}
		if end > len(f.Content) {
			end = len(f.Content)
		}
		if end < start {
			end = start
		}
		code := strings.TrimSpace(f.Content[start:end])

		results = append(results, formatter.PatternMatch{
			ModuleName: module.Name,
			FileName:   f.FileName,
			Match:      code,
			BlockType:  blockType,
			Summary:    "",
		})
	}
	return results
}

type astMatch struct {
	Code      string
	BlockType string
	Summary   string
}

func extractASTPatternMatches(content, pattern string) []astMatch {
	trimmed := strings.TrimSpace(pattern)
	if trimmed == "" {
		return nil
	}

	parser := hclparse.NewParser()
	file, diags := parser.ParseHCL([]byte(content), "temp.tf")
	if diags.HasErrors() {
		return nil
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil
	}

	sliceBlock := func(b *hclsyntax.Block) string {
		rng := b.Range()
		start := rng.Start.Byte
		end := rng.End.Byte
		if start < 0 {
			start = 0
		}
		if end > len(content) {
			end = len(content)
		}
		if end < start {
			end = start
		}
		return strings.TrimSpace(content[start:end])
	}

	var out []astMatch

	hasFilters := parseHasFilters(trimmed)

	if want, ok := getQuotedArg(trimmed, "resource"); ok {
		for _, bl := range body.Blocks {
			if bl.Type == "resource" && len(bl.Labels) >= 2 {
				rtype := bl.Labels[0]
				if strings.HasPrefix(rtype, want) && blockSatisfies(bl.Body, hasFilters) {
					out = append(out, astMatch{Code: sliceBlock(bl), BlockType: "resource", Summary: summarizeAttributes("resource", bl)})
				}
			}
		}
		return out
	}

	if want, ok := getQuotedArg(trimmed, "dynamic"); ok {

		var walk func(bdy *hclsyntax.Body)
		walk = func(bdy *hclsyntax.Body) {
			for _, bl := range bdy.Blocks {
				if bl.Type == "dynamic" && len(bl.Labels) > 0 && bl.Labels[0] == want && blockSatisfies(bl.Body, hasFilters) {
					out = append(out, astMatch{Code: sliceBlock(bl), BlockType: "dynamic", Summary: summarizeAttributes("dynamic", bl)})
				}
				if bl.Body != nil {
					walk(bl.Body)
				}
			}
		}
		walk(body)
		return out
	}

	if strings.HasPrefix(trimmed, "lifecycle") {
		var walk func(bdy *hclsyntax.Body)
		walk = func(bdy *hclsyntax.Body) {
			for _, bl := range bdy.Blocks {
				if bl.Type == "lifecycle" && blockSatisfies(bl.Body, hasFilters) {
					out = append(out, astMatch{Code: sliceBlock(bl), BlockType: "lifecycle", Summary: summarizeAttributes("lifecycle", bl)})
				}
				if bl.Body != nil {
					walk(bl.Body)
				}
			}
		}
		walk(body)
		return out
	}

	return nil
}

func getQuotedArg(pattern, keyword string) (string, bool) {
	prefix := keyword + " "
	if !strings.HasPrefix(pattern, prefix) {
		return "", false
	}
	rest := pattern[len(prefix):]
	first := strings.IndexByte(rest, '"')
	if first < 0 {
		return "", false
	}
	second := strings.IndexByte(rest[first+1:], '"')
	if second < 0 {
		return "", false
	}
	want := strings.TrimSpace(rest[first+1 : first+1+second])
	return want, want != ""
}

func parseHasFilters(pattern string) []string {
	toks := strings.Fields(pattern)
	var filters []string
	for _, t := range toks {
		if rest, ok := strings.CutPrefix(t, "has:"); ok {
			filters = append(filters, rest)
		}
	}
	return filters
}

func blockSatisfies(bdy *hclsyntax.Body, hasFilters []string) bool {
	if len(hasFilters) == 0 {
		return true
	}
	for _, path := range hasFilters {
		if !hasPath(bdy, path) {
			return false
		}
	}
	return true
}

func hasPath(bdy *hclsyntax.Body, path string) bool {
	parts := strings.Split(path, ".")
	return hasPathRec(bdy, parts)
}

func hasPathRec(bdy *hclsyntax.Body, parts []string) bool {
	if len(parts) == 0 {
		return true
	}
	head := parts[0]
	// Attribute present?
	if len(parts) == 1 {
		if _, ok := bdy.Attributes[head]; ok {
			return true
		}
	}
	// Try nested block with this type
	for _, bl := range bdy.Blocks {
		if bl.Type == head {
			if bl.Body != nil && hasPathRec(bl.Body, parts[1:]) {
				return true
			}
		}
	}
	return false
}

func summarizeAttributes(kind string, bl *hclsyntax.Block) string {
	bdy := bl.Body
	keys := make([]string, 0, len(bdy.Attributes))
	for k := range bdy.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return ""
	}
	return fmt.Sprintf("%s attributes: %s", kind, strings.Join(keys, ", "))
}

func extractPatternMatches(content, pattern string) []string {
	var matches []string
	searchContent := content
	offset := 0

	for {
		idx := strings.Index(searchContent, pattern)
		if idx == -1 {
			break
		}

		actualIdx := offset + idx
		startIdx := actualIdx
		for startIdx > 0 && content[startIdx] != '\n' {
			startIdx--
		}

		endIdx := findBlockEnd(content, actualIdx)
		if endIdx > startIdx {
			matches = append(matches, strings.TrimSpace(content[startIdx:endIdx]))
		}

		offset = actualIdx + len(pattern)
		if offset >= len(content) {
			break
		}
		searchContent = content[offset:]
	}

	return matches
}

func findBlockEnd(content string, startPos int) int {
	braceCount := 0
	inBlock := false

	for i := startPos; i < len(content); i++ {
		char := content[i]
		switch char {
		case '{':
			braceCount++
			inBlock = true
		case '}':
			braceCount--
			if inBlock && braceCount == 0 {
				endIdx := i + 1
				for endIdx < len(content) && content[endIdx] != '\n' {
					endIdx++
				}
				return endIdx
			}
		}
	}
	return startPos
}

func paginateResults(results []formatter.PatternMatch, offset, limit int) []formatter.PatternMatch {
	total := len(results)
	startIdx := min(max(0, offset), total)
	endIdx := total
	if limit > 0 {
		endIdx = min(startIdx+limit, total)
	}
	return results[startIdx:endIdx]
}

func (s *Server) handleListModuleExamples(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	moduleArgs, err := UnmarshalArgs[struct {
		ModuleName string `json:"module_name"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid parameters")
	}

	module, err := s.resolveModule(moduleArgs.ModuleName)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Module '%s' not found", moduleArgs.ModuleName))
	}

	files, err := s.db.GetModuleFiles(module.ID)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Error getting files: %v", err))
	}

	exampleMap := buildExampleMap(files)
	text := formatter.ExampleList(module.Name, exampleMap)
	return SuccessResponse(text)
}

func buildExampleMap(files []database.ModuleFile) map[string][]string {
	exampleMap := make(map[string][]string)
	for _, file := range files {
		if strings.HasPrefix(file.FilePath, "examples/") {
			parts := strings.Split(file.FilePath, "/")
			if len(parts) >= 3 {
				exampleName := parts[1]
				exampleMap[exampleName] = append(exampleMap[exampleName], file.FileName)
			}
		}
	}
	return exampleMap
}

func (s *Server) handleGetExampleContent(args any) map[string]any {
	if err := s.ensureDB(); err != nil {
		return ErrorResponse(fmt.Sprintf("Failed to initialize database: %v", err))
	}

	exampleArgs, err := UnmarshalArgs[struct {
		ModuleName  string `json:"module_name"`
		ExampleName string `json:"example_name"`
	}](args)
	if err != nil {
		return ErrorResponse("Error: Invalid parameters")
	}

	module, err := s.resolveModule(exampleArgs.ModuleName)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Module '%s' not found", exampleArgs.ModuleName))
	}

	files, err := s.db.GetModuleFiles(module.ID)
	if err != nil {
		return ErrorResponse(fmt.Sprintf("Error getting files: %v", err))
	}

	exampleFiles := filterExampleFiles(files, exampleArgs.ExampleName)
	if len(exampleFiles) == 0 {
		return ErrorResponse(fmt.Sprintf("Example '%s' not found in module '%s'", exampleArgs.ExampleName, exampleArgs.ModuleName))
	}

	sortedFiles := sortExampleFiles(exampleFiles)
	text := formatter.ExampleContent(module.Name, exampleArgs.ExampleName, sortedFiles)
	return SuccessResponse(text)
}

func filterExampleFiles(files []database.ModuleFile, exampleName string) []database.ModuleFile {
	examplePrefix := fmt.Sprintf("examples/%s/", exampleName)
	var exampleFiles []database.ModuleFile
	for _, file := range files {
		if strings.HasPrefix(file.FilePath, examplePrefix) {
			exampleFiles = append(exampleFiles, file)
		}
	}
	return exampleFiles
}

func sortExampleFiles(files []database.ModuleFile) []database.ModuleFile {
	sortedFiles := make([]database.ModuleFile, 0, len(files))
	var mainFile *database.ModuleFile
	for i := range files {
		if files[i].FileName == "main.tf" {
			mainFile = &files[i]
		} else {
			sortedFiles = append(sortedFiles, files[i])
		}
	}
	if mainFile != nil {
		sortedFiles = append([]database.ModuleFile{*mainFile}, sortedFiles...)
	}
	return sortedFiles
}

func (s *Server) startSyncJob(jobType string, runner func() (*indexer.SyncProgress, error)) *SyncJob {
	jobID := fmt.Sprintf("%s-%d", jobType, time.Now().UnixNano())
	job := &SyncJob{
		ID:        jobID,
		Type:      jobType,
		Status:    "running",
		StartedAt: time.Now(),
	}

	s.jobsMutex.Lock()
	s.jobs[jobID] = job
	s.jobsMutex.Unlock()

	go func() {
		headline := fmt.Sprintf("Sync job %s (%s)", jobID, jobType)
		defer func() {
			if r := recover(); r != nil {
				errMsg := fmt.Sprintf("panic: %v", r)
				log.Printf("%s panicked: %v", headline, r)
				s.completeJobWithError(jobID, errMsg)
			}
		}()

		progress, err := runner()
		if err != nil {
			log.Printf("%s failed: %v", headline, err)
			s.completeJobWithError(jobID, err.Error())
			return
		}

		log.Printf("%s completed", headline)
		s.completeJobWithSuccess(jobID, progress)
	}()

	return job
}

func (s *Server) completeJobWithError(jobID, errMsg string) {
	now := time.Now()
	s.jobsMutex.Lock()
	if job, ok := s.jobs[jobID]; ok {
		job.Status = "failed"
		job.Error = errMsg
		job.CompletedAt = &now
	}
	s.jobsMutex.Unlock()
}

func (s *Server) completeJobWithSuccess(jobID string, progress *indexer.SyncProgress) {
	now := time.Now()
	s.jobsMutex.Lock()
	if job, ok := s.jobs[jobID]; ok {
		job.Status = "completed"
		job.Progress = progress
		job.CompletedAt = &now
	}
	s.jobsMutex.Unlock()
}

func (s *Server) getJob(jobID string) (*SyncJob, bool) {
	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()
	job, ok := s.jobs[jobID]
	return job, ok
}

func (s *Server) listJobs() []*SyncJob {
	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()
	jobs := make([]*SyncJob, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})
	return jobs
}

func (s *Server) formatJobDetails(job *SyncJob) string {
	progressText := ""
	if job.Progress != nil {
		progressText = formatter.SyncProgress(job.Progress)
	}

	return formatter.JobDetails(
		job.ID,
		job.Type,
		job.Status,
		job.StartedAt,
		job.CompletedAt,
		job.Error,
		progressText,
	)
}

func (s *Server) formatJobList(jobs []*SyncJob) string {
	jobInfos := make([]formatter.JobInfo, len(jobs))
	for i, job := range jobs {
		jobInfos[i] = formatter.JobInfo{
			ID:          job.ID,
			Type:        job.Type,
			Status:      job.Status,
			StartedAt:   job.StartedAt,
			CompletedAt: job.CompletedAt,
		}
	}
	return formatter.JobList(jobInfos)
}

func (s *Server) sendResponse(response Message) {
	data, err := json.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return
	}

	if s.writer == nil {
		log.Printf("No writer configured, dropping response: %s", string(data))
		return
	}

	if _, err := fmt.Fprintln(s.writer, string(data)); err != nil {
		log.Printf("Failed to write response: %v", err)
		return
	}
	log.Printf("Sent: %s", string(data))
}

func (s *Server) sendError(code int, message string, id any) {
	response := Message{
		JSONRPC: "2.0",
		ID:      id,
		Error: &RPCError{
			Code:    code,
			Message: message,
		},
	}
	s.sendResponse(response)
}

func (s *Server) resolveModule(nameOrAlias string) (*database.Module, error) {
	if m, err := s.db.GetModule(nameOrAlias); err == nil {
		return m, nil
	}
	if m, err := s.db.ResolveModuleByAlias(nameOrAlias); err == nil {
		return m, nil
	}
	if m, err := s.db.ResolveModuleByAliasPrefix(nameOrAlias); err == nil {
		return m, nil
	}
	mods, err := s.db.SearchModules(nameOrAlias, 1)
	if err == nil && len(mods) > 0 {
		m := mods[0]
		return &m, nil
	}
	return nil, fmt.Errorf("module not found for '%s'", nameOrAlias)
}
