variable "credentials" {
  description = "My Credentials"
  default     = "../../03-sec-vault/shamb0-zoomcamp-lab-01-8d7a5b86610f.json"
}

variable "project" {
  description = "Project"
  default     = "shamb0-zoomcamp-lab-01"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "shamb0_zcamp_2024_hcl_demo_v1_bq_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "shamb0_zcamp_2024_hcl_demo_v1_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}