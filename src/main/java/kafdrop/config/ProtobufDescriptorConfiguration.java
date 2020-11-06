package kafdrop.config;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import kafdrop.util.DeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
public class ProtobufDescriptorConfiguration {
    @Component
    @ConfigurationProperties(prefix = "protobufdesc")
    public static final class ProtobufDescriptorProperties {
        private static final Logger LOG = LoggerFactory.getLogger(ProtobufDescriptorProperties.class);
        // the idea is to let user specifying a directory stored all descriptor file
        // the program will load and .desc file and show as an option on the message
        // detail screen
        private String directory;

        public String getDirectory() {
            return directory;
        }

        public void setDirectory(String directory) {
            this.directory = directory;
        }

        public List<String> getDescFilesList() {
            // getting file list
            if (directory == null || Files.notExists(Path.of(directory))) {
                LOG.info("No descriptor folder configured, skip the setting!!");
                return Collections.emptyList();
            }
            String[] pathnames;
            File path = new File(directory);

            // apply filter for listing only .desc file
            FilenameFilter filter = new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".desc");
                }

            };

            pathnames = path.list(filter);
            return Arrays.asList(pathnames);
        }

        public List<String> getMessageTypes() {
            return getDescFilesList().stream()
                    .flatMap(file -> getMessageTypes(file).stream().map(Descriptors.Descriptor::getName))
                    .collect(Collectors.toList());
        }

        public List<Descriptors.Descriptor> getMessageTypes(String descFile) {
            String fullDescFile = getFullDescFilePath(descFile);
            List<Descriptors.Descriptor> descriptors;
            try (InputStream input = new FileInputStream(fullDescFile)) {
                DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(input);

                List<Descriptors.FileDescriptor> descs = new ArrayList<>();
                for (DescriptorProtos.FileDescriptorProto ffdp : set.getFileList()) {
                    Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(ffdp,
                            (Descriptors.FileDescriptor[]) descs.toArray(new Descriptors.FileDescriptor[descs.size()]));
                    descs.add(fd);
                }

                descriptors = descs.stream().flatMap(desc -> desc.getMessageTypes().stream()).collect(Collectors.toList());
            } catch (FileNotFoundException e) {
                final String errorMsg = "Couldn't open descriptor file: " + fullDescFile;
                LOG.error(errorMsg, e);
                throw new DeserializationException(errorMsg);
            } catch (IOException e) {
                final String errorMsg = "Can't parse descriptor file";
                LOG.error(errorMsg, e);
                throw new DeserializationException(errorMsg);
            } catch (Descriptors.DescriptorValidationException e) {
                final String errorMsg = "Can't compile proto message";
                LOG.error(errorMsg, e);
                throw new DeserializationException(errorMsg);
            }
            return descriptors;
        }

        String getFullDescFilePath(String descFile) {
            return getDirectory() + File.separator + descFile;
        }
    }
}
