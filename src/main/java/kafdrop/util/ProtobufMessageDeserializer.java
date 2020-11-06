package kafdrop.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;

public class ProtobufMessageDeserializer implements MessageDeserializer {

    private String topic;
    private String fullDescFile;
    private String msgTypeName;

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufMessageDeserializer.class);

    public ProtobufMessageDeserializer(String topic, String fullDescFile, String msgTypeName) {
        this.topic = topic;
        this.fullDescFile = fullDescFile;
        this.msgTypeName = msgTypeName;
    }

    @Override
    public String deserializeMessage(ByteBuffer buffer) {

        try {
            final var descriptors = getMessageTypes(fullDescFile);

            final var messageDescriptor = descriptors.stream()
                    .filter(desc -> msgTypeName.equals(desc.getName()))
                    .findFirst();
            if (messageDescriptor.isEmpty()) {
                final String errorMsg = "Can't find specific message type: " + msgTypeName;
                LOG.error(errorMsg);
                throw new DeserializationException(errorMsg);
            }
            DynamicMessage message = DynamicMessage.parseFrom(messageDescriptor.get(), CodedInputStream.newInstance(buffer));

            JsonFormat.TypeRegistry typeRegistry = JsonFormat.TypeRegistry.newBuilder().add(descriptors).build();
            Printer printer = JsonFormat.printer().usingTypeRegistry(typeRegistry);

            return printer.print(message).replaceAll("\n", ""); // must remove line break so it defaults to collapse mode
        } catch (FileNotFoundException e) {
            final String errorMsg = "Couldn't open descriptor file: " + fullDescFile;
            LOG.error(errorMsg, e);
            throw new DeserializationException(errorMsg);
        } catch (IOException e) {
            final String errorMsg = "Can't decode Protobuf message";
            LOG.error(errorMsg, e);
            throw new DeserializationException(errorMsg);
        }
    }
    public static List<Descriptors.Descriptor> getMessageTypes(String descFile) {
//        String fullDescFile = getDirectory() + File.separator + descFile;
        List<Descriptors.Descriptor> descriptors;
        try (InputStream input = new FileInputStream(descFile)) {
            DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(input);

            List<Descriptors.FileDescriptor> descs = new ArrayList<>();
            for (DescriptorProtos.FileDescriptorProto ffdp : set.getFileList()) {
                Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(ffdp,
                        (Descriptors.FileDescriptor[]) descs.toArray(new Descriptors.FileDescriptor[descs.size()]));
                descs.add(fd);
            }

            descriptors = descs.stream().flatMap(desc -> desc.getMessageTypes().stream()).collect(Collectors.toList());
        } catch (FileNotFoundException e) {
            final String errorMsg = "Couldn't open descriptor file: " + descFile;
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
}
