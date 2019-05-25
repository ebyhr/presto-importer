/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.presto.io;

import io.airlift.airline.Option;

public class ClientOptions
{
    @Option(name = "--server", title = "server", description = "Presto server location (default: localhost:8080)")
    public String server = "localhost:8080";

    @Option(name = "--user", title = "user", description = "Username")
    public String user = System.getProperty("user.name");

    @Option(name = "--password", title = "password", description = "Prompt for password")
    public boolean password;

    @Option(name = "--table", title = "table", description = "Table")
    public String table;

    @Option(name = {"--batch-size"}, title = "batch-size", description = "Max batch size")
    public int batchSize = 1000;

    @Option(name = "--drop-create", title = "drop-create", description = "Drop and create a table")
    public boolean dropCreate = true;

    @Option(name = {"-f", "--file"}, title = "file", description = "Execute statements from file and exit")
    public String file;

    @Option(name = "--input-format", title = "input-format", description = "Input format for batch mode [CSV, TSV, CSV_UNQUOTED] (default: CSV_UNQUOTED)")
    public FileFormat inputFormat = FileFormat.CSV_UNQUOTED;

    @Option(name = "--ignore-errors", title = "ignore errors", description = "Continue processing in batch mode when an error occurs (default is to exit immediately)")
    public boolean ignoreErrors;

    public enum FileFormat
    {
        TSV('\0', '\t'),
        CSV('"', ','),
        CSV_UNQUOTED('\0', ',');

        private final char quote;
        private final char separator;

        FileFormat(char quote, char separator)
        {
            this.quote = quote;
            this.separator = separator;
        }

        public char getQuote()
        {
            return quote;
        }

        public char getSeparator()
        {
            return separator;
        }
    }
}
