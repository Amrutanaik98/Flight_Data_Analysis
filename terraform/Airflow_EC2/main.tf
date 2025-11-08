terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "airflow_sg" {
  name        = "airflow-sg"
  description = "Security group for Airflow"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${var.your_ip}/32"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "airflow-sg"
  }
}

resource "aws_iam_role" "airflow_role" {
  name = "airflow-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "s3_access" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "sqs_access" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSQSFullAccess"
}

resource "aws_iam_role_policy_attachment" "dynamodb_access" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_access" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchFullAccess"
}

resource "aws_iam_instance_profile" "airflow_profile" {
  name = "airflow-instance-profile"
  role = aws_iam_role.airflow_role.name
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.micro"
  key_name               = var.key_pair_name
  iam_instance_profile   = aws_iam_instance_profile.airflow_profile.name
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }

  user_data = base64encode(file("${path.module}/user_data.sh"))

  monitoring = true

  tags = {
    Name = "airflow-server"
  }

  depends_on = [aws_security_group.airflow_sg]
}

resource "aws_eip" "airflow_eip" {
  instance = aws_instance.airflow.id
  domain   = "vpc"

  tags = {
    Name = "airflow-eip"
  }

  depends_on = [aws_instance.airflow]
}

output "instance_public_ip" {
  description = "Your EC2 Public IP Address"
  value       = aws_eip.airflow_eip.public_ip
}

output "airflow_ui_url" {
  description = "Airflow Website URL"
  value       = "http://${aws_eip.airflow_eip.public_ip}:8080"
}

output "ssh_command" {
  description = "SSH command to connect"
  value       = "ssh -i ~/.ssh/airflow-key.pem ubuntu@${aws_eip.airflow_eip.public_ip}"
}