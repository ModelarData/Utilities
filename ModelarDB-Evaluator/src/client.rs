/* Copyright 2023 The ModelarData Utilities Contributors
 *
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

//! Client for ingesting and querying modelardbd.

use arrow::compute;
use arrow::datatypes::{DataType, Fields, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::{FlightDataEncoder, FlightDataEncoderBuilder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightClient, FlightDescriptor, Ticket};
use bytes::Bytes;
use futures::TryStreamExt;

use crate::TABLE_NAME;

/// The host and port for the client to connect to.
const HOST_AND_PORT: &str = "grpc://127.0.0.1:9999";

/// Wrapper for [`FlightClient`] so that ingestion and retrieval of data points can be implemented
/// as methods instead of as simple functions that explicitly take a [`FlightClient`] as input.
pub struct Client {
    flight_client: FlightClient,
}

impl Client {
    /// Create [`FlightClient`] that connects to [`HOST_AND_PORT`].
    pub async fn new() -> Result<Self, FlightError> {
        let flight_service_client = FlightServiceClient::connect(HOST_AND_PORT)
            .await
            .map_err(|error| FlightError::ExternalError(Box::new(error)))?;

        Ok(Client {
            flight_client: FlightClient::new_from_inner(flight_service_client),
        })
    }

    /// Create a model table named [`TABLE_NAME`] with the schema of `uncompressed_data` on the
    /// instance of modelardbd at [`HOST_AND_PORT`] and then ingest `uncompressed_data` into it. The
    /// field columns in the model table will be created with `error_bound`.
    pub async fn ingest_uncompressed_data(
        &mut self,
        uncompressed_data: RecordBatch,
        error_bound: f32,
    ) -> Result<(), FlightError> {
        self.create_model_table(uncompressed_data.schema().fields(), error_bound)
            .await?;

        let flight_data_stream = Self::create_flight_data_from_record_batch(uncompressed_data);

        self.flight_client.do_put(flight_data_stream).await?;

        self.flight_client
            .do_action(Action {
                r#type: "FlushMemory".to_owned(),
                body: Bytes::new(),
            })
            .await?;

        Ok(())
    }

    /// Create a model table named [`TABLE_NAME`] with the columns in `fields` on the instance of
    /// modelardbd at [`HOST_AND_PORT`]. The field columns in the model table will be created with
    /// `error_bound`.
    async fn create_model_table(
        &mut self,
        fields: &Fields,
        error_bound: f32,
    ) -> Result<(), FlightError> {
        let mut columns = Vec::with_capacity(fields.size());
        for field in fields {
            let column = match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, None) => {
                    format!("{} TIMESTAMP", field.name())
                }
                DataType::Float32 => format!("{} FIELD({error_bound})", field.name()),
                DataType::Utf8 => format!("{} TAG", field.name()),
                data_type => Err(FlightError::NotYetImplemented(format!(
                    "Only Timestamp, Float, and String are supported, {data_type} is not."
                )))?,
            };

            columns.push(column);
        }

        let sql = format!("CREATE MODEL TABLE {TABLE_NAME} ({})", columns.join(", "));
        let action = Action::new("CommandStatementUpdate", sql);
        self.flight_client.do_action(action).await?;

        Ok(())
    }

    fn create_flight_data_from_record_batch(uncompressed_data: RecordBatch) -> FlightDataEncoder {
        let flight_descriptor = FlightDescriptor::new_path(vec![TABLE_NAME.to_owned()]);
        let record_batch_stream = futures::stream::iter([Ok(uncompressed_data)]);

        FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(record_batch_stream)
    }

    /// Retrieve the `timestamp_column` and `value_column` in the model table named [`TABLE_NAME`]
    /// from the instance of modelardbd at [`HOST_AND_PORT`]. The returned `RecordBatch` will be
    /// sorted by `timestamp_column` in ascending order.
    pub async fn retrieve_decompressed_data(
        &mut self,
        timestamp_column: &str,
        value_column: &str,
    ) -> Result<RecordBatch, FlightError> {
        let ticket = Ticket {
            ticket: format!(
                "SELECT {timestamp_column}, {value_column} \
                 FROM {TABLE_NAME} ORDER BY {timestamp_column}"
            )
            .into(),
        };

        // Execute query.
        let query_result: Vec<RecordBatch> = self
            .flight_client
            .do_get(ticket)
            .await?
            .try_collect()
            .await?;

        compute::concat_batches(&query_result[0].schema(), &query_result)
            .map_err(|error| FlightError::Arrow(error))
    }
}
