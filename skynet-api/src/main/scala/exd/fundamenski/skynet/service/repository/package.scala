package exd.fundamenski.skynet.service

import exd.fundamenski.skynet.service.config.RepoConfig
import ml.combust.mleap.executor.error.BundleException
import cats.data.{NonEmptyList, Validated}
import cats.kernel.Semigroup
import zio.{Has, Layer, RIO, Task, UIO, ZIO, ZLayer, ZManaged}
import zio.blocking.Blocking

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, StandardCopyOption}
import zio.blocking._
import cats.implicits._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import com.google.cloud.storage.Storage
import org.log4s.getLogger

import scala.util.Try

package object repository {

  type Checked[URI] = Validated[URIError, URI]

  type Repository = Has[Repository.Service]

  object Repository extends Serializable {
    trait Service extends Serializable {

      type ServiceURI

      def name: String

      /** Download the bundle specified by the URI
       * to a local file.
       *
       * @param uri uri of the bundle to download
       * @return future of the local file path after completion
       */
      protected def download(uri: ServiceURI): RIO[Blocking, Path]

      /** Whether this repository can handle the given URI.
       *
       * @param uri uri for the bundle
       * @return true if it can handle, false otherwise
       */
      def validateUri(uri: java.net.URI): Checked[ServiceURI]

      def downloadFromUri(uri: java.net.URI): RIO[Blocking, Path] =
        for {
          request   <- Task.fromTry(validateUri(uri).toEither.toTry)
          path      <- download(request)
        } yield path

      def canHandle(uri: java.net.URI): Boolean = validateUri(uri).isValid
    }

    object Service {

      val live: Service = MultiRepository(
        FileRepository,
        S3Repository,
        GoogleStorageRepository
      )

      case class MultiRepository(repositories: NonEmptyList[Repository.Service]) extends Repository.Service {

        val name: String = "MultiRepository"

        override type ServiceURI = java.net.URI

        override def download(uri: URI): RIO[Blocking, Path] =
          ZIO.fromEither {
            repositories.reduceLeftTo(repo => repo.validateUri(uri).map(repo.download))(
              (v, repo) => v.findValid(repo.validateUri(uri).map(repo.download))
            ).toEither
          }.flatten

        override def downloadFromUri(uri: URI): RIO[Blocking, Path] =
          download(uri)

        override def validateUri(uri: URI): Checked[ServiceURI] =
          repositories.reduceLeftTo(_.validateUri(uri))((v, u) => v.findValid(u.validateUri(uri))).as(uri)
      }
      object MultiRepository {

        def apply(repo: Repository.Service, repos: Repository.Service*): MultiRepository =
          MultiRepository(NonEmptyList(repo, repos.toList))
      }

      /**
       * Load bundle from local filesystem
       * @param move
       */
      object FileRepository extends Repository.Service {

        override val name: String = "FileRepository"

        override type ServiceURI = java.net.URI

        override def download(uri: URI): RIO[Blocking, Path] = blocking(RIO.effectSuspend {

          if (uri.getPath.isEmpty) throw new BundleException("file path cannot be empty")

          val local = new File(uri.getPath).toPath

          if (!Files.exists(local)) throw new BundleException(s"file does not exist $local")

          localFolder(uri).map { path =>
            val tmpFile = path.toNIO
            Files.copy(local, tmpFile, StandardCopyOption.REPLACE_EXISTING)
            tmpFile
          }
        })

        override def validateUri(uri: URI): Checked[URI] =
          if (uri.getScheme == "file" || uri.getScheme == "jar:file") {
            uri.valid
          } else {
            URIError.Reason(uri, name, "Scheme must be either 'file' or 'jar:file'").invalid
          }
      }


      object S3Repository extends Repository.Service {

        val name: String = "S3Repository"

        override type ServiceURI = AmazonS3URI

        private[this] val logger = getLogger(name)

        private val managedClient: ZManaged[Blocking, Throwable, AmazonS3] =
          ZManaged.makeEffect {
            logger.info("Creating S3Repository.client")
            AmazonS3ClientBuilder.defaultClient()
          } { client =>
            logger.info("Shutting down S3Repository.client")
            client.shutdown()
          }

        override def download(uri: ServiceURI): RIO[Blocking, Path] =
          managedClient.use(client =>
          for {
            modelFile <- localFolder(uri.getURI)
            file <- effectBlockingInterrupt {
              val obj = client.getObject(uri.getBucket, uri.getKey).getObjectContent
              Files.copy(obj, modelFile.toNIO, StandardCopyOption.REPLACE_EXISTING)
              modelFile.toNIO
            }
          } yield file)

        override def validateUri(uri: URI): Checked[AmazonS3URI] =
          Try(new AmazonS3URI(uri)).toValidated.leftMap(URIError.Cause(uri, name, _))
      }

      object GoogleStorageRepository extends Repository.Service {

        val name: String = "GoogleStorageRepository"

        private[this] val logger = getLogger(name)

        override type ServiceURI = GoogleStorageURI

        import com.google.cloud.storage.StorageOptions

        private lazy val client: Storage = {
          logger.info("Creating GoogleStorageRepository.client")
          StorageOptions.getDefaultInstance.getService
        }

        override def validateUri(uri: URI): Checked[GoogleStorageURI] = GoogleStorageURI(uri)

        override def download(uri: GoogleStorageURI): RIO[Blocking, Path] = for {

          _         <- UIO(logger.info(s"Fetching ${uri.path} from ${uri.bucket}"))

          bytes     <- effectBlockingInterrupt(client.readAllBytes(uri.bucket, uri.path)).disconnect

          file      <- localFolder(uri.parentUri)

          _         <- effectBlockingInterrupt(os.write.over(
            target = file,
            data = bytes,
            createFolders = true
          ))

        } yield file.toNIO
      }

      abstract case class GoogleStorageURI private (bucket: String, path: String) {
        val parentUri: URI
        override def toString: String = parentUri.toString
      }

      object GoogleStorageURI {

        val name = "GoogleStorageRepository"

        def apply(uri: URI): Checked[GoogleStorageURI] =
          (uri.getScheme match {
            case "gs" => uri.valid[URIError]
            case other => URIError.Reason(uri, name,
              s"Invalid scheme found: $other. Scheme must be 'gs://...'").invalid
          }) *> Try(new GoogleStorageURI(uri.getHost, uri.getRawPath.tail) {
            override val parentUri: URI = uri
          })
            .toValidated
            .leftMap(URIError.Cause(uri, name, _))
      }
    }

    val fromConfig: ZLayer[Has[RepoConfig], Nothing, Repository] =
      ZLayer.fromService(repoConfigToService)

    val any: ZLayer[Repository, Nothing, Repository] =
      ZLayer.requires[Repository]

    val live: Layer[Nothing, Repository] =
      ZLayer.succeed(Service.live)


    def repoConfigToService(repoConfig: RepoConfig): Repository.Service =
      repoConfig match {
        case RepoConfig.S3            => Service.S3Repository
        case RepoConfig.File          => Service.FileRepository
        case RepoConfig.GS            => Service.GoogleStorageRepository
        case RepoConfig.Multi(repos)  => Service.MultiRepository(repos.map(repoConfigToService))
      }

    private def localFolder(uri: URI): Task[os.Path] = Task {
      val path = os.pwd / uri.hashCode().toString / os.Path(uri.getPath).segments.toList.last
      path.toIO.deleteOnExit()
      path
    }
  }
}

sealed trait URIError extends Error with Product with Serializable {
  def input: URI
}
object URIError {
  final case class Cause(input: URI, repo: String, cause: Throwable) extends URIError {
    override def getMessage: String = s"$repo: URI error for '$input': ${cause.getMessage}"
  }
  final case class Reason(input: URI, repo: String, reason: String) extends URIError {
    override def getMessage: String = s"$repo: URI error for '$input': $reason"
  }
  final case class Multiple(input: URI, errors: NonEmptyList[URIError]) extends URIError {
    override def getMessage: String = s"Multiple URI errors \n ${errors.map(_.getMessage).toList.mkString("\n")}"
  }

  implicit val semigroupURIParseError: Semigroup[URIError] = Semigroup.instance {
    case (Multiple(input, errors), Multiple(_, b))  => Multiple(input, errors concatNel b)
    case (Multiple(input, errors), other)           => Multiple(input, errors append other)
    case (other, Multiple(input, errors))           => Multiple(input, errors prepend other)
    case (a, b)                                     => Multiple(a.input, NonEmptyList(a, List(b)))
  }
}
