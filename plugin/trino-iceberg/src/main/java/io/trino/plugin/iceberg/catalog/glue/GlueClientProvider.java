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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.glue.AWSGlueAsync;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class GlueClientProvider
        implements Provider<AWSGlueAsync>
{
    private final AWSGlueAsync glueClient;

    @Inject
    public GlueClientProvider(
            GlueMetastoreStats stats,
            AWSCredentialsProvider credentialsProvider,
            GlueHiveMetastoreConfig glueConfig,
            IcebergGlueCatalogConfig icebergGlueConfig)
    {
        requireNonNull(stats, "stats is null");
        requireNonNull(credentialsProvider, "credentialsProvider is null");
        glueClient = createAsyncGlueClient(glueConfig, credentialsProvider, Optional.of(new SkipArchiveRequestHandler(icebergGlueConfig.isSkipArchive())), stats.newRequestMetricsCollector());
    }

    @Override
    public AWSGlueAsync get()
    {
        return glueClient;
    }
}
