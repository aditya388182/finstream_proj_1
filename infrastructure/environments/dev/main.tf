provider "aws" {
  region = "us-east-1"
}

# The Passport: Tells AWS to accept your local public key
resource "aws_key_pair" "finstream_key" {
  key_name   = "finstream-key-v2" 
  public_key = file("~/.ssh/finstream_key.pub")
}

resource "aws_security_group" "finstream_sg" {
  name        = "finstream_sg"
  description = "Allow SSH, Kafka, and Airflow"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Open port 8080 for the Airflow UI
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

  # Open port 3000 for Grafana Dashboard
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Open port 9090 for Prometheus Metrics
  ingress {
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# NEW: Dynamically fetch the latest official Ubuntu 22.04 AMI
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical's official AWS account ID

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "finstream_node" {
  # We now use the dynamic Ubuntu AMI fetched above
  ami           = data.aws_ami.ubuntu.id
  instance_type = "t3.xlarge" 
  
  key_name      = aws_key_pair.finstream_key.key_name
  associate_public_ip_address = true 

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  vpc_security_group_ids = [aws_security_group.finstream_sg.id]

  tags = {
    Name = "FinStream-Remote-Engine"
  }

  # The automation script to boot the factory
  user_data = <<-EOF
              #!/bin/bash
              # Update and install Docker
              apt-get update
              apt-get install -y docker.io docker-compose-v2 git
              systemctl start docker
              systemctl enable docker
              usermod -aG docker ubuntu
              
              # Clone the repository
              cd /home/ubuntu
              git clone https://github.com/aditya388182/finstream_proj_1.git pipeline
              
              # Set permissions so Docker can map volumes correctly
              chown -R ubuntu:ubuntu /home/ubuntu/pipeline
              chmod -R 777 /home/ubuntu/pipeline/data
              
              # Launch the infrastructure
              cd /home/ubuntu/pipeline
              docker compose -f docker/docker-compose.yml up -d
              docker compose -f docker/airflow-compose.yml up -d
              EOF
}

output "instance_public_ip" {
  value = aws_instance.finstream_node.public_ip
}