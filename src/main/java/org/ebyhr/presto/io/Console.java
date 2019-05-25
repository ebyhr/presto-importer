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

import au.com.bytecode.opencsv.CSVReader;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;

import javax.inject.Inject;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

@Command(name = "presto-io", description = "Presto io console")
public class Console
{
    @Inject
    public HelpOption helpOption;

    @Inject
    public VersionOption versionOption = new VersionOption();

    @Inject
    public ClientOptions clientOptions = new ClientOptions();

    public boolean run()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(clientOptions.table);
        String path = clientOptions.file;
        ClientOptions.FileFormat format = clientOptions.inputFormat;

        try {
            QueryRunner queryRunner = new QueryRunner(
                    clientOptions.server,
                    clientOptions.user,
                    clientOptions.password ? Optional.of(getPassword()) : Optional.empty());

            if (clientOptions.dropCreate) {
                queryRunner.dropCreateTable(table);
            }

            Map<String, String> columns = queryRunner.getColumns(table);

            long rows = 0;
            Instant start = Instant.now();
            try (Reader reader = Files.newBufferedReader(Paths.get(path));
                    CSVReader csvReader = new CSVReader(reader, format.getSeparator(), format.getQuote())) {
                List<String[]> records = new ArrayList<>();
                String[] record;
                while ((record = csvReader.readNext()) != null) {
                    records.add(record);
                    if (records.size() == clientOptions.batchSize) {
                        queryRunner.insertRows(table, columns, records);
                        records.clear();
                    }
                    rows++;
                }
                if (!records.isEmpty()) {
                    queryRunner.insertRows(table, columns, records);
                }
            }
            Instant end = Instant.now();
            Duration duration = Duration.between(start, end);
            System.out.println("FINISHED");
            System.out.println(format("%s seconds [%s rows]", duration.getSeconds(), rows));
        }
        catch (ClassNotFoundException | SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    private String getPassword()
    {
        checkState(clientOptions.user != null, "Username must be specified along with password");
        String defaultPassword = System.getenv("PRESTO_PASSWORD");
        if (defaultPassword != null) {
            return defaultPassword;
        }

        java.io.Console console = System.console();
        if (console == null) {
            throw new RuntimeException("No console from which to read password");
        }
        char[] password = console.readPassword("Password: ");
        if (password != null) {
            return new String(password);
        }
        return "";
    }
}
