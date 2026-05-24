provider "aws" {
  region = "us-east-1"
}

# The Passport: Tells AWS to accept your local public key
resource "aws_key_pair" "finstream_key" {
  key_name   = "finstream-key-v2"  # Renaming to force AWS to create a new entry
  public_key = file("~/.ssh/finstream_key.pub")
}

resource "aws_security_group" "finstream_sg" {
  name        = "finstream_sg"
  description = "Allow SSH and Kafka"

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

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "finstream_node" {
  ami           = "ami-0453ec754f44f9a4a"
  instance_type = "t3.xlarge" # Upgraded to 16GB RAM Supercomputer
  
  # The Bridge: Links your key pair to this specific server
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
}

output "instance_public_ip" {
  value = aws_instance.finstream_node.public_ip
}