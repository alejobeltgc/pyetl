#!/usr/bin/env python3
"""
Script de deployment completamente en Python
"""
import boto3
import zipfile
import os
import json
import time
from pathlib import Path


class PyETLDeployer:
    def __init__(self, region='us-east-1'):
        self.region = region
        self.session = boto3.Session(region_name=region)
        self.s3 = self.session.client('s3')
        self.lambda_client = self.session.client('lambda')
        self.dynamodb = self.session.client('dynamodb')
        self.apigateway = self.session.client('apigateway')
        self.iam = self.session.client('iam')
        
        # Configuración del proyecto
        self.project_name = 'pyetl'
        self.bucket_name = f'{self.project_name}-input-{self.get_account_id()}-{region}'
        self.table_name = f'{self.project_name}-rates'
        
    def get_account_id(self):
        """Obtener el account ID de AWS"""
        sts = self.session.client('sts')
        return sts.get_caller_identity()['Account']
    
    def create_s3_bucket(self):
        """Crear bucket S3 para archivos de entrada"""
        try:
            if self.region == 'us-east-1':
                self.s3.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region}
                )
            
            # Habilitar versionado
            self.s3.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            
            print(f"✅ Bucket S3 creado: {self.bucket_name}")
            return True
            
        except Exception as e:
            if 'BucketAlreadyExists' in str(e) or 'BucketAlreadyOwnedByYou' in str(e):
                print(f"✅ Bucket S3 ya existe: {self.bucket_name}")
                return True
            print(f"❌ Error creando bucket S3: {e}")
            return False
    
    def create_dynamodb_table(self):
        """Crear tabla DynamoDB"""
        try:
            self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'pk', 'KeyType': 'HASH'},
                    {'AttributeName': 'sk', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'pk', 'AttributeType': 'S'},
                    {'AttributeName': 'sk', 'AttributeType': 'S'},
                    {'AttributeName': 'business_line', 'AttributeType': 'S'},
                    {'AttributeName': 'last_updated', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST',
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'business-line-index',
                        'KeySchema': [
                            {'AttributeName': 'business_line', 'KeyType': 'HASH'},
                            {'AttributeName': 'last_updated', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            )
            
            # Esperar a que la tabla esté activa
            print("⏳ Esperando a que la tabla DynamoDB esté activa...")
            waiter = self.dynamodb.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)
            
            print(f"✅ Tabla DynamoDB creada: {self.table_name}")
            return True
            
        except Exception as e:
            if 'ResourceInUseException' in str(e):
                print(f"✅ Tabla DynamoDB ya existe: {self.table_name}")
                return True
            print(f"❌ Error creando tabla DynamoDB: {e}")
            return False
    
    def create_lambda_role(self):
        """Crear rol IAM para Lambda"""
        role_name = f'{self.project_name}-lambda-role'
        
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        lambda_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream", 
                        "logs:PutLogEvents"
                    ],
                    "Resource": "arn:aws:logs:*:*:*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectVersion"
                    ],
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:UpdateItem",
                        "dynamodb:DeleteItem"
                    ],
                    "Resource": [
                        f"arn:aws:dynamodb:{self.region}:{self.get_account_id()}:table/{self.table_name}",
                        f"arn:aws:dynamodb:{self.region}:{self.get_account_id()}:table/{self.table_name}/index/*"
                    ]
                }
            ]
        }
        
        try:
            # Crear rol
            self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=f'Rol para las funciones Lambda de {self.project_name}'
            )
            
            # Crear y adjuntar política
            policy_name = f'{self.project_name}-lambda-policy'
            self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(lambda_policy),
                Description=f'Política para las funciones Lambda de {self.project_name}'
            )
            
            self.iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn=f'arn:aws:iam::{self.get_account_id()}:policy/{policy_name}'
            )
            
            print(f"✅ Rol IAM creado: {role_name}")
            return f'arn:aws:iam::{self.get_account_id()}:role/{role_name}'
            
        except Exception as e:
            if 'EntityAlreadyExists' in str(e):
                print(f"✅ Rol IAM ya existe: {role_name}")
                return f'arn:aws:iam::{self.get_account_id()}:role/{role_name}'
            print(f"❌ Error creando rol IAM: {e}")
            return None
    
    def package_lambda_code(self):
        """Empaquetar código de Lambda"""
        zip_path = Path('lambda-deployment.zip')
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Agregar archivos fuente
            src_path = Path('src')
            for file_path in src_path.rglob('*.py'):
                zipf.write(file_path, file_path.relative_to(Path('.')))
            
            # Agregar config.py
            if Path('config.py').exists():
                zipf.write('config.py')
        
        print(f"✅ Código Lambda empaquetado: {zip_path}")
        return zip_path
    
    def deploy_lambda_functions(self, role_arn):
        """Desplegar funciones Lambda"""
        zip_path = self.package_lambda_code()
        
        with open(zip_path, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        functions = [
            {
                'name': f'{self.project_name}-etl-processor',
                'handler': 'lambda_handler.process_rates_file',
                'timeout': 900,  # 15 minutos
                'memory': 1024
            },
            {
                'name': f'{self.project_name}-health-check',
                'handler': 'lambda_handler.health_check',
                'timeout': 30,
                'memory': 128
            },
            {
                'name': f'{self.project_name}-api',
                'handler': 'api_handler.handler',
                'timeout': 30,
                'memory': 256
            }
        ]
        
        deployed_functions = {}
        
        for func in functions:
            try:
                response = self.lambda_client.create_function(
                    FunctionName=func['name'],
                    Runtime='python3.9',
                    Role=role_arn,
                    Handler=func['handler'],
                    Code={'ZipFile': zip_content},
                    Timeout=func['timeout'],
                    MemorySize=func['memory'],
                    Environment={
                        'Variables': {
                            'DYNAMODB_TABLE': self.table_name,
                            'S3_BUCKET': self.bucket_name
                        }
                    }
                )
                deployed_functions[func['name']] = response['FunctionArn']
                print(f"✅ Función Lambda creada: {func['name']}")
                
            except Exception as e:
                if 'ResourceConflictException' in str(e):
                    # Actualizar función existente
                    self.lambda_client.update_function_code(
                        FunctionName=func['name'],
                        ZipFile=zip_content
                    )
                    
                    response = self.lambda_client.get_function(FunctionName=func['name'])
                    deployed_functions[func['name']] = response['Configuration']['FunctionArn']
                    print(f"✅ Función Lambda actualizada: {func['name']}")
                else:
                    print(f"❌ Error con función Lambda {func['name']}: {e}")
        
        # Limpiar archivo temporal
        zip_path.unlink()
        return deployed_functions
    
    def setup_s3_trigger(self, etl_function_arn):
        """Configurar trigger S3 para Lambda"""
        try:
            # Dar permisos a S3 para invocar Lambda
            self.lambda_client.add_permission(
                FunctionName=etl_function_arn,
                StatementId='s3-trigger-permission',
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{self.bucket_name}'
            )
            
            # Configurar notificación S3
            notification_config = {
                'LambdaConfigurations': [
                    {
                        'Id': 'xlsx-upload-trigger',
                        'LambdaFunctionArn': etl_function_arn,
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {'Name': 'suffix', 'Value': '.xlsx'}
                                ]
                            }
                        }
                    }
                ]
            }
            
            self.s3.put_bucket_notification_configuration(
                Bucket=self.bucket_name,
                NotificationConfiguration=notification_config
            )
            
            print("✅ Trigger S3 configurado")
            return True
            
        except Exception as e:
            if 'ResourceConflictException' in str(e):
                print("✅ Trigger S3 ya existe")
                return True
            print(f"❌ Error configurando trigger S3: {e}")
            return False
    
    def deploy(self):
        """Ejecutar deployment completo"""
        print("🚀 Iniciando deployment de PyETL...")
        
        # Crear recursos
        if not self.create_s3_bucket():
            return False
            
        if not self.create_dynamodb_table():
            return False
        
        role_arn = self.create_lambda_role()
        if not role_arn:
            return False
        
        # Esperar un poco para que el rol se propague
        print("⏳ Esperando propagación del rol IAM...")
        time.sleep(10)
        
        functions = self.deploy_lambda_functions(role_arn)
        if not functions:
            return False
        
        # Configurar trigger S3
        etl_function_name = f'{self.project_name}-etl-processor'
        if etl_function_name in functions:
            self.setup_s3_trigger(functions[etl_function_name])
        
        print("\n🎉 Deployment completado!")
        print(f"📁 Bucket S3: {self.bucket_name}")
        print(f"🗄️ Tabla DynamoDB: {self.table_name}")
        print(f"⚡ Funciones Lambda: {len(functions)} desplegadas")
        
        return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Desplegar PyETL en AWS')
    parser.add_argument('--region', default='us-east-1', help='Región de AWS')
    
    args = parser.parse_args()
    
    deployer = PyETLDeployer(region=args.region)
    deployer.deploy()
