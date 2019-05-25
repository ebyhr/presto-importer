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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CatalogSchemaTableName
{
    private final String catalog;
    private final String schema;
    private final String table;

    public CatalogSchemaTableName(String catalogSchemaTable)
    {
        requireNonNull(catalogSchemaTable);
        checkArgument(catalogSchemaTable.split("\\.").length == 3);
        String[] split = catalogSchemaTable.split("\\.");
        this.catalog = split[0];
        this.schema = split[1];
        this.table = split[2];
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    @Override
    public String toString()
    {
        return format("%s.%s.%s", catalog, schema, table);
    }
}
