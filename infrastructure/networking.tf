# -----------------------------------------------------------------------------
# VPC, Subnets, NAT Gateway, and VPC Endpoints
#
# Only created when enable_vpc = true (required for DB2/MSSQL connectivity).
# Glue JDBC Connections and Lambda functions that access databases need
# to run inside a VPC with routes to the database hosts.
# -----------------------------------------------------------------------------

resource "aws_vpc" "main" {
  count                = var.enable_vpc ? 1 : 0
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${local.project_prefix}-vpc"
  }
}

resource "aws_subnet" "private" {
  count             = var.enable_vpc ? length(var.private_subnet_cidrs) : 0
  vpc_id            = aws_vpc.main[0].id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]

  tags = {
    Name = "${local.project_prefix}-private-${count.index}"
  }
}

# -----------------------------------------------------------------------------
# Internet Gateway + NAT (Lambda in VPC needs NAT for Secrets Manager, S3)
# -----------------------------------------------------------------------------

resource "aws_internet_gateway" "main" {
  count  = var.enable_vpc ? 1 : 0
  vpc_id = aws_vpc.main[0].id

  tags = {
    Name = "${local.project_prefix}-igw"
  }
}

resource "aws_eip" "nat" {
  count  = var.enable_vpc ? 1 : 0
  domain = "vpc"

  tags = {
    Name = "${local.project_prefix}-nat-eip"
  }
}

resource "aws_nat_gateway" "main" {
  count         = var.enable_vpc ? 1 : 0
  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.private[0].id

  tags = {
    Name = "${local.project_prefix}-nat"
  }

  depends_on = [aws_internet_gateway.main]
}

resource "aws_route_table" "private" {
  count  = var.enable_vpc ? 1 : 0
  vpc_id = aws_vpc.main[0].id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main[0].id
  }

  tags = {
    Name = "${local.project_prefix}-private-rt"
  }
}

resource "aws_route_table_association" "private" {
  count          = var.enable_vpc ? length(var.private_subnet_cidrs) : 0
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[0].id
}

# -----------------------------------------------------------------------------
# VPC Endpoints — avoid NAT charges for AWS service traffic
# -----------------------------------------------------------------------------

resource "aws_vpc_endpoint" "s3" {
  count        = var.enable_vpc ? 1 : 0
  vpc_id       = aws_vpc.main[0].id
  service_name = "com.amazonaws.${local.region}.s3"

  route_table_ids = [aws_route_table.private[0].id]

  tags = {
    Name = "${local.project_prefix}-vpce-s3"
  }
}

resource "aws_vpc_endpoint" "secretsmanager" {
  count             = var.enable_vpc ? 1 : 0
  vpc_id            = aws_vpc.main[0].id
  service_name      = "com.amazonaws.${local.region}.secretsmanager"
  vpc_endpoint_type = "Interface"
  subnet_ids        = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpc_endpoints[0].id]

  private_dns_enabled = true

  tags = {
    Name = "${local.project_prefix}-vpce-secrets"
  }
}

# -----------------------------------------------------------------------------
# Security Groups
# -----------------------------------------------------------------------------

resource "aws_security_group" "glue" {
  count       = var.enable_vpc ? 1 : 0
  name        = "${local.project_prefix}-glue-sg"
  description = "Security group for Glue JDBC connections"
  vpc_id      = aws_vpc.main[0].id

  # Glue self-referencing rule (required for Glue jobs in a VPC)
  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.project_prefix}-glue-sg"
  }
}

resource "aws_security_group" "lambda" {
  count       = var.enable_vpc ? 1 : 0
  name        = "${local.project_prefix}-lambda-sg"
  description = "Security group for Lambda functions accessing databases"
  vpc_id      = aws_vpc.main[0].id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.project_prefix}-lambda-sg"
  }
}

resource "aws_security_group" "vpc_endpoints" {
  count       = var.enable_vpc ? 1 : 0
  name        = "${local.project_prefix}-vpce-sg"
  description = "Security group for VPC endpoints"
  vpc_id      = aws_vpc.main[0].id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.project_prefix}-vpce-sg"
  }
}
