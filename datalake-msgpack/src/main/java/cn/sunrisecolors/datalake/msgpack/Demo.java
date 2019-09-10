package cn.sunrisecolors.datalake.msgpack;

import com.fasterxml.jackson.core.io.JsonEOFException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author shaohongliang
 * @since 2019/9/9 15:04
 */
public class Demo {
    public static void main(String[] args) throws IOException {

        // Instantiate ObjectMapper for MessagePack
//        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
        ObjectMapper objectMapper = new ObjectMapper();

        // Serialize a Java object to byte array
        ExamplePojo pojo = new ExamplePojo("komamitsu");
        byte[] bytes = objectMapper.writeValueAsBytes(pojo);

        // Deserialize the byte array to a Java object
        try {
            ExamplePojo deserialized = objectMapper.readValue(bytes, ExamplePojo.class);
            System.out.println(deserialized.getName()); // => komamitsu
        }catch (JsonEOFException e){
            System.out.println(((JsonEOFException)e).getProcessor().currentName() + " 值被破坏");
        }
    }

}
