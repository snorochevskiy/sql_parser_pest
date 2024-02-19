package Dependencies

case class EmrRelease(emr: String, awsSdk: String, spark: String)

object EmrRelease {
  val `Emr-6.10.0` = EmrRelease(emr = "6.10.0",
    awsSdk = "1.12.397", spark = "3.3.1"
  )

  val `Emr-6.11.0` = EmrRelease(emr = "6.11.0",
    awsSdk = "1.12.446", spark = "3.3.2"
  )

  val `Emr-6.12.0` = EmrRelease(emr = "6.12.0",
    awsSdk = "1.12.490", spark = "3.4.0"
  )

  val `Emr-6.13.0` = EmrRelease(emr = "6.13.0",
    awsSdk = "1.12.513", spark = "3.4.1"
  )

  val `Emr-6.15.0` = EmrRelease(emr = "6.15.0",
    awsSdk = "1.12.569", spark = "3.4.1"
  )
}
