package postgres

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/joho/godotenv"
)

type Client struct {
	executable  string
	database    string
	environment []string
	secrets     []string
}

func NewClient(target, envFile string) (*Client, error) {
	settings := make(map[string]string)
	if envFile != "" {
		values, err := godotenv.Read(envFile)
		if err != nil {
			return nil, fmt.Errorf("cannot read environment file; check its path and dotenv syntax")
		}
		for key, value := range values {
			settings[strings.ToUpper(key)] = value
		}
	}
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		settings[strings.ToUpper(key)] = value
	}
	variable := ""
	switch target {
	case "direct":
		variable = "GENGO_DIRECT_DATABASE_URL"
	case "shared":
		variable = "GENGO_TRANSACTION_DATABASE_URL"
	case "dedicated":
		variable = "GENGO_DEDICATED_DATABASE_URL"
	case "libpq":
	default:
		return nil, fmt.Errorf("target must be direct, shared, dedicated, or libpq")
	}
	client := &Client{executable: "psql"}
	if variable != "" {
		if settings[variable] == "" {
			return nil, fmt.Errorf("set %s for the %s target", variable, target)
		}
		parsed, err := url.Parse(settings[variable])
		if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
			return nil, fmt.Errorf("%s must be a PostgreSQL connection URL", variable)
		}
		if parsed.User != nil {
			if password, present := parsed.User.Password(); present {
				settings["PGPASSWORD"] = password
			}
			parsed.User = url.User(parsed.User.Username())
		}
		parameters, err := url.ParseQuery(parsed.RawQuery)
		if err != nil {
			return nil, fmt.Errorf("%s contains invalid URL parameters", variable)
		}
		for parameter, environment := range map[string]string{"password": "PGPASSWORD", "sslpassword": "PGSSLPASSWORD"} {
			if parameters.Has(parameter) {
				settings[environment] = parameters.Get(parameter)
				parameters.Del(parameter)
			}
		}
		parsed.RawQuery = parameters.Encode()
		client.database = parsed.String()
	}
	for key, value := range settings {
		client.environment = append(client.environment, key+"="+value)
		if value == "" {
			continue
		}
		if key == "PGPASSWORD" || key == "PGSSLPASSWORD" || strings.HasSuffix(key, "_DATABASE_URL") || (key == "PGDATABASE" && strings.ContainsAny(value, "=:/")) {
			client.secrets = append(client.secrets, value)
			if parsed, err := url.Parse(value); err == nil && parsed.User != nil {
				if password, present := parsed.User.Password(); present && password != "" {
					client.secrets = append(client.secrets, password, url.QueryEscape(password), url.PathEscape(password))
				}
			}
		}
	}
	sort.Slice(client.secrets, func(first, second int) bool {
		return len(client.secrets[first]) > len(client.secrets[second])
	})
	return client, nil
}

func (client *Client) Run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	options := []string{"-X", "-w", "--set=ON_ERROR_STOP=on"}
	if client.database != "" {
		options = append(options, "--dbname", client.database)
	}
	return client.run(ctx, client.executable, append(options, arguments...), stdout, stderr)
}

func (client *Client) run(ctx context.Context, executable string, arguments []string, stdout, stderr io.Writer) error {
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Env = client.environment
	var output, diagnostics bytes.Buffer
	command.Stdout = &output
	command.Stderr = &diagnostics
	err := command.Run()
	if _, writeErr := io.WriteString(stdout, client.redact(output.String())); writeErr != nil {
		return writeErr
	}
	if _, writeErr := io.WriteString(stderr, client.redact(diagnostics.String())); writeErr != nil {
		return writeErr
	}
	if ctx.Err() != nil {
		return fmt.Errorf("PostgreSQL operation stopped: %w", ctx.Err())
	}
	if err != nil {
		return fmt.Errorf("%s failed; check client installation, connection settings, and diagnostics: %w", executable, err)
	}
	return nil
}

func (client *Client) redact(value string) string {
	for _, secret := range client.secrets {
		value = strings.ReplaceAll(value, secret, "[REDACTED]")
	}
	return value
}
