variable "aws_region" {
  default = "us-east-1"
}

variable "instance_type" {
  default = "t2.small"
}

variable "key_pair_name" {
  type = string
}

variable "your_ip" {
  type = string
}