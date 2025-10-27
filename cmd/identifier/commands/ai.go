package commands

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
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
	"golang.org/x/exp/maps"
)

type fileList []*identifier.FileData
type resultList []*identifier.AIResultStruct
type folderT struct {
	NumberOfFiles int
	FileExcerpt   fileList
	SubFolders    []string
	FolderName    string
}

func (f *folderT) maxExcerpts(num int) {
	if num > len(f.FileExcerpt) {
		return
	}
	for i := len(f.FileExcerpt) - 1; i >= num; i-- {
		index := rand.Intn(i + 1)
		f.FileExcerpt = append(f.FileExcerpt[:index], f.FileExcerpt[index+1:]...)
	}
	return
}

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
var aiMaxFiles int64

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
	aiCmd.Flags().Int64Var(&aiMaxFiles, "max-files", 8, "maximum number of files per folder")
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

func addUnique[S interface{ ~[]E }, E cmp.Ordered](x S, target E) S {
	pos, found := slices.BinarySearch(x, target)
	if found {
		return x
	}
	x = slices.Insert(x, pos, target)
	return x
}

func addFolderFolderUnique(list map[string]*folderT, folder string, subfolder string) {
	if _, ok := list[folder]; !ok {
		list[folder] = &folderT{
			NumberOfFiles: 0,
			FileExcerpt:   fileList{},
			SubFolders:    []string{},
			FolderName:    folder,
		}
	}
	list[folder].SubFolders = addUnique(list[folder].SubFolders, subfolder)
}

func addFolderFileUnique(list map[string]*folderT, folder string, file *identifier.FileData) {
	if _, ok := list[folder]; !ok {
		list[folder] = &folderT{
			NumberOfFiles: 0,
			FileExcerpt:   fileList{},
			SubFolders:    []string{},
			FolderName:    folder,
		}
	}
	pos, found := slices.BinarySearchFunc(list[folder].FileExcerpt, file, func(data *identifier.FileData, data2 *identifier.FileData) int {
		return cmp.Compare(data.Basename, data2.Basename)
	})
	if found {
		return
	}
	list[folder].FileExcerpt = slices.Insert(list[folder].FileExcerpt, pos, file)
	list[folder].NumberOfFiles++
	return
}

func doAi(cmd *cobra.Command, args []string) {
	if matches := envRegexp.FindStringSubmatch(apikeyAIFlag); len(matches) > 1 {
		apikeyAIFlag = os.Getenv(matches[1])
	}
	if aiQuery == "" {
		aiQuery = `Erstelle Metadaten basierend auf de "INPUT-JSON" Metadaten für jeden Folder, der in "FOLDERLIST-JSON" gelistet ist. Stelle sicher, dass jeder Folder genau einmal auftaucht.
Falls Folder oder Dateinamen Semantik beinhalten, Nutze diese für Titel und Beschreibung. Sollten Folder oder Dateinamen Rückschlüsse auf Ort oder Datum zulassen,
fülle diese Felder entsprechend aus. Date bitte im Format "YYYY-MM-DD"" oder "YYYY"", Zeiträume werden durch "YYYY - YYYY"" dargestellt. Verwende keine anderen Datierungsformate und keine Buchstaben. Achte darauf, dass die Metadaten in der JSON-Datei im korrekten Format vorliegen. 
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
		cedaPlugin := &oai.OpenAICompatible{
			Provider: provider,
			APIKey:   apikeyAIFlag,
			BaseURL:  "https://llm-api-h200.ceda.unibas.ch/litellm",
		}
		plugins = append(plugins, cedaPlugin)
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

	flow := genkit.DefineFlow(g, "identifier", func(ctx context.Context, input []*folderT) (*resultList, error) {
		inputBytes, err := json.Marshal(input)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshal input: %v", input)
		}
		folerList := []string{}
		for _, i := range input {
			folerList = append(folerList, i.FolderName)
		}
		folderBytes, err := json.Marshal(folerList)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshal input: %v", folerList)
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
	// input := fileList{}
	// folderList0 := []string{}
	folderList := map[string]*folderT{}
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
					logger.Error().Msgf("no indexer data for '%s'", fData.Path)
					return nil
				}
				fData.Indexer.Metadata = map[string]any{}
				fData.Path = ""
				fData.LastSeen = 0
				fData.LastMod = 0
				fData.Folder = filepath.ToSlash(fData.Folder)
				addFolderFileUnique(folderList, fData.Folder, fData)

				parts := strings.Split(fData.Folder, "/")
				folder := ""
				for _, part := range parts {
					newFolder := path.Join(folder, part)
					addFolderFolderUnique(folderList, folder, part)
					//folderList0 = addUnique(folderList0, newFolder)
					folder = newFolder
				}

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
	folderList0 := maps.Keys(folderList)
	logger.Info().Msgf("writing %d folders to csv", len(folderList0))
	last := int64(len(folderList0))/aiResultFolder + 1
	for i := int64(0); i < last; i++ {
		j := min(i*aiResultFolder+aiResultFolder, int64(len(folderList0)))
		if j <= i*aiResultFolder {
			continue
		}
		logger.Info().Msgf("querying %s", modelAIFlag)

		input := []*folderT{}
		for k := i * aiResultFolder; k < j; k++ {
			x := folderList[folderList0[k]]
			x.maxExcerpts(int(aiMaxFiles))
			input = append(input, x)
		}
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
