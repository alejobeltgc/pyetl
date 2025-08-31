#!/usr/bin/env python3
"""
Infrastructure as Code for PyETL using AWS CDK
"""
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigateway,
    aws_s3_notifications as s3n,
    aws_iam as iam,
    Duration,
    RemovalPolicy
)
from constructs import Construct


class PyETLStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 Bucket para archivos de entrada
        input_bucket = s3.Bucket(
            self, "PyETLInputBucket",
            bucket_name=f"pyetl-input-{self.account}-{self.region}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,  # Solo para desarrollo
            auto_delete_objects=True  # Solo para desarrollo
        )

        # DynamoDB Table
        rates_table = dynamodb.Table(
            self, "RatesTable",
            table_name="pyetl-rates",
            partition_key=dynamodb.Attribute(
                name="pk",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="sk", 
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,  # Solo para desarrollo
            time_to_live_attribute="ttl"
        )

        # Global Secondary Index para consultas por business_line
        rates_table.add_global_secondary_index(
            index_name="business-line-index",
            partition_key=dynamodb.Attribute(
                name="business_line",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="last_updated",
                type=dynamodb.AttributeType.STRING
            )
        )

        # Lambda Layer con pandas y dependencias
        pandas_layer = _lambda.LayerVersion(
            self, "PandasLayer",
            code=_lambda.Code.from_asset("layers/pandas-layer.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
            description="Pandas and data processing libraries"
        )

        # Lambda function para procesar archivos
        etl_lambda = _lambda.Function(
            self, "ETLProcessorFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_handler.process_rates_file",
            code=_lambda.Code.from_asset("src"),
            timeout=Duration.minutes(15),
            memory_size=1024,
            layers=[pandas_layer],
            environment={
                "DYNAMODB_TABLE": rates_table.table_name,
                "S3_BUCKET": input_bucket.bucket_name
            }
        )

        # Lambda function para health check
        health_lambda = _lambda.Function(
            self, "HealthCheckFunction", 
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_handler.health_check",
            code=_lambda.Code.from_asset("src"),
            timeout=Duration.seconds(30),
            memory_size=128
        )

        # Lambda function para API
        api_lambda = _lambda.Function(
            self, "APIFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="api_handler.handler",
            code=_lambda.Code.from_asset("src"),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "DYNAMODB_TABLE": rates_table.table_name
            }
        )

        # Permisos para Lambda
        rates_table.grant_read_write_data(etl_lambda)
        rates_table.grant_read_data(api_lambda)
        input_bucket.grant_read(etl_lambda)

        # S3 trigger para Lambda
        input_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(etl_lambda),
            s3.NotificationKeyFilter(suffix=".xlsx")
        )

        # API Gateway
        api = apigateway.RestApi(
            self, "PyETLAPI",
            rest_api_name="PyETL Rates API",
            description="API para consultar datos procesados por PyETL"
        )

        # Health check endpoint
        health_integration = apigateway.LambdaIntegration(health_lambda)
        api.root.add_resource("health").add_method("GET", health_integration)

        # API endpoints
        api_integration = apigateway.LambdaIntegration(api_lambda)
        
        # /documents/{document_id}
        documents = api.root.add_resource("documents")
        documents.add_method("GET", api_integration)  # List all documents
        
        document = documents.add_resource("{document_id}")
        document.add_method("GET", api_integration)  # Get specific document
        
        # /business-lines/{business_line}/services
        business_lines = api.root.add_resource("business-lines")
        business_line = business_lines.add_resource("{business_line}")
        services = business_line.add_resource("services")
        services.add_method("GET", api_integration)

        # Outputs
        cdk.CfnOutput(self, "InputBucketName", value=input_bucket.bucket_name)
        cdk.CfnOutput(self, "DynamoDBTableName", value=rates_table.table_name)
        cdk.CfnOutput(self, "APIEndpoint", value=api.url)


app = cdk.App()
PyETLStack(app, "PyETLStack")
app.synth()
