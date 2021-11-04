package org.jvcode.mvnplugin;

import static java.io.File.separatorChar;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.nio.file.Files.copy;
import static java.nio.file.Path.of;
import static java.util.stream.Collectors.toUnmodifiableSet;

import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.InstantiationStrategy;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 *
 * <h1><b>Copy File Plugin</b></h1>
 *
 * <p>
 * Plugin que permite la copia de n ficheros en n directorios.<br>
 * Se utilizan threads para acelerar la ejecuci&oacute;n, el n&uacute;mero de
 * threads por defecto es uno, si se requiere el uso de m&aacute;s threads
 * existe la posibilidad de pasar ese dato por par&aacute;metro como property en
 * el POM.xml.<br>
 * <br>
 * <em><b>*La copia de los ficheros se realiza en cada uno de los
 * directorios.</b></em>
 * </p>
 * 
 * <h2>Listado de properties</h2>
 * <p>
 * <em><b>*Todos los path se separan por punto y coma [;]</b></em>
 * </p>
 * <div>
 * <ul>
 * <li>copyfile.dir.targets [alias=targets, readonly, required]</li>
 * <li>copyfile.file.sources [alias=sources, readonly, required]</li>
 * <li>copyfile.threads [alias = threads, default = 1, readonly]</li>
 * </ul>
 * </div>
 * 
 * 
 * @author Javier P&eacute;rez Alonso
 *
 *         29 oct. 2021
 *
 */
@Mojo(name = "copy", requiresProject = false, defaultPhase = LifecyclePhase.NONE, instantiationStrategy = InstantiationStrategy.SINGLETON)
public class CopyFilePlugin extends AbstractMojo
{

      @Parameter(property = "dir.targets", required = true, readonly = true)
      private String dirTarget;
      @Parameter(property = "file.sources", required = true, readonly = true)
      private String fileSrc;
      @Parameter(property = "threads", required = false, readonly = true, defaultValue = "1")
      private int threads;

      private static final String OK_FILE_PREFIX_MSG = "Es un archivo aceptado a copiar--> ";
      private static final String OK_DIR_PREFIX_MSG = "Es un directorio aceptado donde copiar--> ";
      private static final String SEPARATOR_FILE = ";";

      private ExecutorService executor;
      private final Log LOGGER = getLog();

      private static final Predicate<Path> isFile = path-> path.toFile()
                                                               .isFile();
      private static final Predicate<Path> isDirectory = path-> path.toFile()
                                                                    .isDirectory();

      /**
       * <h2>[Method execute]</h2><br>
       *
       * Se sobrescribe el m&eacute;todo execute, en este m&eacute;todo se ejecutan
       * todas las operaciones.
       * 
       * @throws MojoExecutionException
       *
       */
      @Override
      public void execute() throws MojoExecutionException
      {

            try
            {
                  validateThreadsParam();
                  var sources = getPaths(this.fileSrc,isFile,OK_FILE_PREFIX_MSG);
                  int sourcesSize = sources.size();
                  LOGGER.info(format("Son un total de %s archivos distintos a copiar%n",sourcesSize));
                  validateSize(sourcesSize,"Archivos fuente");
                  var targets = getPaths(this.dirTarget,isDirectory,OK_DIR_PREFIX_MSG);
                  int targetsSize = targets.size();
                  LOGGER.info(format("Son un total de %s directorios distintos donde copiar%n",targetsSize));
                  validateSize(targetsSize,"Directorios objetivo");
                  executor = Executors.newFixedThreadPool(adjust(threads,sourcesSize));
                  sources.forEach(path-> targets.forEach(pathTarget-> copyFile(path,pathTarget)));
                  Function<Path,Stream<Callable<Path>>> flatMapper = source-> targets.stream()
                                                                                     .map(target-> copyFile(source,target));
                  var tasks = sources.stream()
                                     .flatMap(flatMapper)
                                     .collect(toUnmodifiableSet());

                  executor.invokeAll(tasks);
                  executor.shutdown();
            }
            catch (Exception ex)
            {
                  throw new MojoExecutionException("Error al ejecutar el plugin.",ex);
            }
      }

      /**
       * 
       * <h2>[Method validateThreadsParam]</h2><br>
       * 
       * En este m&eacute;todo se valida el par&aacute;metro threads, en d&oacute;nde
       * se indica la cantidad de hilos a utilizar, por defecto siendo 1. <b>Debe ser
       * mayor a 0</b>.
       *
       * @throws Exception
       */
      public void validateThreadsParam() throws Exception
      {
            int availableProcessors = getRuntime().availableProcessors();
            if (threads <= 0 || threads > availableProcessors)
            {

                  throw new Exception(format("Lo threads usados por el plugin deben ser mayores a 0 y menores a %s, que son los available processors para la JVM.",
                                             availableProcessors));
            }
      }

      /**
       * 
       * <h2>[Method validateThreadsParam]</h2><br>
       * 
       * En este m&eacute;todo se genera un {@link Callable} en el que se copia un
       * archivo dentro de un directorio.
       *
       * @param path       : Path del archivo a copiar.
       * @param pathTarget : Path del directorio d&oacute;nde se va a copiar.
       * @return Un {@link Callable} quer devolver&aacute; un {@link Path}.
       */
      public Callable<Path> copyFile(Path path,Path pathTarget)
      {
            return ()->
                  {
                        Path fileName = path.getFileName();
                        try
                        {
                              var pathPostCopy = copy(path,of(formatTargetPath(pathTarget,fileName)),StandardCopyOption.REPLACE_EXISTING);
                              LOGGER.info(format("Se ha realizado la copia --> %s%n",pathPostCopy));
                              return pathPostCopy;
                        }
                        catch (Exception ex)
                        {
                              LOGGER.error(format("No se pudo copiar el archivo-> -- %s -- en el dir --> %s%nSe sigue con el proceso%n",
                                                  fileName,pathTarget));
                        }
                        return null;
                  };
      }

      /**
       * 
       * <h2>[Method validateThreadsParam]</h2><br>
       * 
       * En este m&eacute;todo se formatea el path de directorio objetivo concatenando
       * el nombre del archivo a copiar, se comprueba si el pathtarget acaba en slash,
       * permitiendo setear el path en la property del POM de ambas formas.
       * 
       * @param pathTarget : Path objetivo.
       * @param fileName   : Nombre del archivo.
       * @return Un {@link String} con el path formateado.
       */
      public String formatTargetPath(Path pathTarget,Path fileName)
      {
            var target = pathTarget.toString();
            var targetOk = target.charAt(target.length() - 1) == separatorChar ? target : target + separatorChar;
            return targetOk + fileName;
      }

      /**
       * 
       * <h2>[Method getPaths]</h2><br>
       * 
       * En este m&eacute;todo se devuelve un {@link Set} de tipo {@link Path} post
       * separaci&oacute; de los token que incluye el string obtenido del contenido de
       * la property del POM.
       *
       * @param propertyStr : String con el property content.
       * @param isValid     : {@link Predicate} de tipo {@link Path} que valida los
       *                    path.
       * @param okPrefixMsg : String con el prefijo de un mensaje de log.
       * @return Un {@link Set} inmutable con todos los Path v&aacute;lidos.
       */
      public Set<Path> getPaths(String propertyStr,Predicate<Path> isValid,String okPrefixMsg)
      {
            return Stream.of(propertyStr.split(SEPARATOR_FILE))
                         .map(Path::of)
                         .filter(isValid)
                         .peek(path-> LOGGER.info(format("%s%s%n",okPrefixMsg,path.getFileName())))
                         .collect(toUnmodifiableSet());
      }

      private void validateSize(int setSize,String set) throws Exception
      {
            if (setSize < 1)
            {
                  throw new Exception(format("%s existentes deben ser al menos uno",set));
            }
      }

      /**
       * <h2>[Method adjust]</h2><br>
       * 
       * En este m&eacute;todo se ajusta el n&uacute;mero de threads utilizados, para
       * los casos que se pase un valor mayor al n&uacute;mero de archivos a copiar.
       *
       * @param threads     : N&uacute;mero de hilos actual.
       * @param sourcesSize : Longitud del set de archivos.
       * @return n&uacute;mero de hilos a utilizar en el executor.
       */
      private int adjust(int threads,int sourcesSize)
      {
            var threadsNum = threads > sourcesSize ? sourcesSize : threads;
            LOGGER.info(format("Finalmente se usan %s threads para realizar la tarea",threadsNum));
            return threadsNum;
      }
}
