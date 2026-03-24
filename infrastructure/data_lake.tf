# -----------------------------------------------------------------------------
# S3 Data Bucket
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  bucket = "${local.project_prefix}-data-${local.account_id}-${local.region}"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "archive-old-versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = 90
      storage_class   = "GLACIER"
    }
  }

  rule {
    id     = "cleanup-incomplete-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# -----------------------------------------------------------------------------
# S3 Scripts Bucket
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "scripts" {
  bucket = "${local.project_prefix}-scripts-${local.account_id}-${local.region}"
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# Glue Data Catalog Databases
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_database" "main" {
  name        = "${local.name_prefix}_db"
  description = "SAS migration data catalog"
}

resource "aws_glue_catalog_database" "raw" {
  name        = "${local.name_prefix}_raw"
  description = "SAS migration raw zone"
}

resource "aws_glue_catalog_database" "processed" {
  name        = "${local.name_prefix}_processed"
  description = "SAS migration processed zone"
}

resource "aws_glue_catalog_database" "archive" {
  name        = "${local.name_prefix}_archive"
  description = "SAS migration archive zone"
}
