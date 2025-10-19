package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"emperror.dev/errors"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"
	oai "github.com/firebase/genkit/go/plugins/compat_oai"
	"github.com/firebase/genkit/go/plugins/compat_oai/openai"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/identifier/identifier"
	"github.com/spf13/cobra"
)

var aiCmd = &cobra.Command{
	Use:     "ai",
	Aliases: []string{},
	Short:   "retrieves AI descriptions",
	Long:    `retrieves AI descriptions`,
	Example: ``,
	Args:    cobra.NoArgs,
	Run:     doAi,
}

var dbFolderAIFlag string
var prefixAIFlag string
var csvAIFlag string
var jsonlAIFlag string
var xlsxAIFlag string
var consoleAIFlag bool
var modelAIFlag string
var apikeyAIFlag string
var aiQuery string
var aiAdditionalQuery string
var aiResultFolder int64

func aiInit() {
	aiCmd.Flags().StringVar(&dbFolderAIFlag, "database", "", "folder for database (must already exist)")
	aiCmd.Flags().StringVar(&prefixAIFlag, "prefix", "", "folder path prefix")
	aiCmd.Flags().StringVar(&csvAIFlag, "csv", "", "write ai to csv file")
	aiCmd.Flags().StringVar(&jsonlAIFlag, "jsonl", "", "write ai to jsonl file")
	aiCmd.Flags().StringVar(&xlsxAIFlag, "xlsx", "", "write ai to xlsx file (needs memory)")
	aiCmd.Flags().BoolVar(&consoleAIFlag, "console", false, "write ai to console")
	aiCmd.Flags().StringVar(&modelAIFlag, "model", "googleai/gemini-2.5-flash", "model for ai")
	aiCmd.Flags().StringVar(&apikeyAIFlag, "apikey", "%%GEMINI_API_KEY%%", "apikey for ai")
	aiCmd.Flags().StringVar(&aiQuery, "query", "", "query for ai")
	aiCmd.Flags().Int64Var(&aiResultFolder, "result-folder", 50, "folder number for result, if 0, all folders are used")
	aiCmd.Flags().StringVar(&aiAdditionalQuery, "additional-query", "", "additional query for ai, will be prepended to the main query")
	aiCmd.MarkFlagDirname("database")
	aiCmd.MarkFlagRequired("database")
	aiCmd.MarkFlagFilename("jsonl", "jsonl", "json")
	aiCmd.MarkFlagFilename("csv", "csv")
	aiCmd.MarkFlagFilename("xlsx", "xlsx")

	aiRoCrateInit()
	aiCmd.AddCommand(aiRoCrateCmd)
	aiListInit()
	aiCmd.AddCommand(aiListCmd)
}

var envRegexp = regexp.MustCompile(`%%([A-Z0-9_]+)%%`)

var regexpJSON = regexp.MustCompile(`(?s)^[^{\[]*([{\[].*[}\]])[^}\]]*$`)

var fieldsAI = []string{"folder", "title", "description", "place", "date", "tags", "persons", "institutions"}

func doAi(cmd *cobra.Command, args []string) {
	if matches := envRegexp.FindStringSubmatch(apikeyAIFlag); len(matches) > 1 {
		apikeyAIFlag = os.Getenv(matches[1])
	}
	if aiQuery == "" {
		aiQuery = `Erstelle Metadaten basierend auf de "INPUT-JSON" Metadaten für jeden Folder, der in "FOLDERLIST-JSON" gelistet ist. Stelle sicher, dass jeder Folder genau einmal auftaucht.
Falls Folder oder Dateinamen Semantik beinhalten, Nutze diese für Titel und Beschreibung. Sollten Folder oder Dateinamen Rückschlüsse auf Ort oder Datum zulassen,
fülle diese Felder entsprechend aus. Date bitte im Format YYYY-MM-DD oder YYYY, Zeiträume werden durch YYYY - YYYY dargestellt. Achte darauf, dass die Metadaten in der JSON-Datei im korrekten Format vorliegen. 
Die Felder "place" und "date" sind optional, aber sollten ausgefüllt werden, wenn die Informationen verfügbar sind.
Befülle das Tags Feld mit den Tags, die für den jeweiligen Folder relevant sind. Mögliche Tags sind: "audio", "video", "text", "image", "unknown".
Die Liste "Persons" enthält die Struktur Person, welche im Feld "Name" in der Art "Nachname, Vorname" (Vorname optional) aufgeführt sind und optional im Feld "Role" noch die Rolle.
Institutionen sollten im Feld "Institutions" aufgeführt werden, falls sie vorhanden sind. Institutions ist ein String Array. Syntax für Institution ist "Name, Ort" (Ort optional).
Die Metadaten sollten in englischer Sprache verfasst sein.
Sprache ist Englisch und der Duktus wissenschaftlich. Achte darauf, dass das JSON Format korrekt eingehalten wird.`
	} else if fi, err := os.Stat(aiQuery); err == nil && !fi.IsDir() {
		// read query from file
		data, err := os.ReadFile(aiQuery)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot read query from file '%s'", aiQuery)
			defer os.Exit(1)
			return
		}
		aiQuery = string(data)
	}
	if aiAdditionalQuery != "" {
		if fi, err := os.Stat(aiAdditionalQuery); err == nil && !fi.IsDir() {
			// read additional query from file
			data, err := os.ReadFile(aiAdditionalQuery)
			if err != nil {
				logger.Error().Err(err).Msgf("cannot read additional query from file '%s'", aiAdditionalQuery)
				defer os.Exit(1)
				return
			}
			aiAdditionalQuery = string(data)
		}
		aiQuery = aiAdditionalQuery + "\n\n" + aiQuery
	}
	modelAIFlag = strings.ToLower(modelAIFlag)
	modelParts := strings.SplitN(modelAIFlag, "/", 2)
	if len(modelParts) != 2 {
		logger.Error().Msgf("model '%s' must consist of driver- and modelname", modelAIFlag)
		defer os.Exit(1)
		return
	}
	var plugins = []api.Plugin{}
	provider := modelParts[0]
	switch provider {
	case "ceda":
		plugins = append(plugins, &oai.OpenAICompatible{
			Provider: provider,
			APIKey:   apikeyAIFlag,
			BaseURL:  "https://llm-api-h200.ceda.unibas.ch/litellm",
		})
	case "openai":
		plugins = append(plugins, &openai.OpenAI{})
	case "googleai":
		plugins = append(plugins, &googlegenai.GoogleAI{})
	default:
		logger.Fatal().Msgf("unknown provider '%s'", provider)
	}
	g := genkit.Init(
		context.Background(),
		genkit.WithPlugins(plugins...),
		genkit.WithDefaultModel(modelAIFlag),
	)

	var badgerDB *badger.DB

	output, err := identifier.NewOutput(consoleAIFlag || (csvAIFlag == "" && jsonlAIFlag == "" && xlsxAIFlag == ""),
		csvAIFlag, jsonlAIFlag, xlsxAIFlag, "ai", fieldsAI, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot create output")
		defer os.Exit(1)
		return
	}
	defer func() {
		if err := output.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close output")
		}
	}()

	if badgerDB, err = badger.Open(badger.DefaultOptions(dbFolderAIFlag).WithCompression(badgerOptions.Snappy).WithLogger(zLogger.NewZWrapper(logger))); err != nil {
		logger.Error().Err(err).Msgf("cannot open badger database in '%s'", dbFolderFlag)
		defer os.Exit(1)
		return
	}
	defer badgerDB.Close()

	type fileList []*identifier.FileData
	type resultList []*identifier.AIResultStruct
	flow := genkit.DefineFlow(g, "identifier", func(ctx context.Context, input fileList) (*resultList, error) {
		folderList := []string{}
		for _, file := range input {
			folderList = append(folderList, file.Folder)
		}
		slices.Sort(folderList)
		folderList = slices.Compact(folderList)
		folderBytes, err := json.Marshal(folderList)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshal folderlist: %v", folderList)
		}
		inputBytes, err := json.Marshal(input)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshal input: %v", input)
		}
		// Create a prompt based on the input
		prompt := fmt.Sprintf("%s\n---\nINPUT-JSON: %s\n---\nFOLDERLIST-JSON: %s", aiQuery, string(inputBytes), string(folderBytes))

		// Generate structured recipe data using the same schema
		resultData, _, err := genkit.GenerateData[resultList](ctx, g,
			ai.WithPrompt(prompt),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to generate recipe: %w", err)
		}

		return resultData, nil
	})

	var prefix = "file:" + prefixAIFlag
	input := fileList{}
	folderList := []string{}
	if err := badgerDB.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = true
		options.Prefix = []byte(prefix)
		it := txn.NewIterator(options)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(val []byte) error {
				fData := &identifier.FileData{}
				if err := json.Unmarshal(val, fData); err != nil {
					return errors.Wrapf(err, "cannot unmarshal file data from key '%s'", k)
				}
				if fData.Indexer == nil {
					return errors.Errorf("no indexer data for '%s'", fData.Path)
				}
				fData.Indexer.Metadata = map[string]any{}
				fData.Path = ""
				fData.LastSeen = 0
				fData.LastMod = 0
				folderList = append(folderList, fData.Folder)
				input = append(input, fData)
				return nil
			}); err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	}); err != nil {
		logger.Error().Err(err).Msgf("cannot iterate over badger database with prefix '%s'", prefixAIFlag)
		defer os.Exit(1)
		return
	}
	slices.Sort(folderList)
	folderList = slices.Compact(folderList)
	logger.Info().Msgf("writing %d files to csv", len(folderList))
	last := int64(len(folderList))/aiResultFolder + 1
	for i := int64(0); i < last; i++ {
		j := min(i*aiResultFolder+aiResultFolder, int64(len(folderList)))
		if j <= i*aiResultFolder {
			continue
		}
		logger.Info().Msgf("querying %s", modelAIFlag)

		out, err := flow.Run(context.TODO(), input)
		if err != nil {
			logger.Error().Err(err).Msg("cannot query ai")
			defer os.Exit(1)
			return
		}
		if err := badgerDB.Update(func(txn *badger.Txn) error {
			for _, r := range *out {
				data, err := json.Marshal(r)
				if err != nil {
					return errors.Wrapf(err, "cannot marshal result for '%s'", r.Folder)
				}
				if err := txn.Set([]byte(fmt.Sprintf("ai:%s:%s", modelAIFlag, r.Folder)), data); err != nil {
					return errors.Wrapf(err, "cannot write result for '%s'", r.Folder)
				}
				persons := ""
				for _, p := range r.Persons {
					if p.Name != "" {
						persons += p.Name
					}
					if p.Role != "" {
						persons += " (" + p.Role + ")"
					}
					persons += " -- "
				}
				persons = strings.TrimSuffix(persons, " -- ")
				institutions := strings.Join(r.Institutions, " -- ")
				output.Write([]any{r.Folder, r.Title, r.Description, r.Place, r.Date, strings.Join(r.Tags, ";"), persons, institutions}, r)
			}
			return nil
		}); err != nil {
			logger.Error().Err(err).Msg("cannot write result to badger")
		}
	}

	return
}
