package nn;

import nn.util.FileOperator;

import java.io.File;

public class NNApp {
    public static void main(String[] args){
        FileOperator divider = new FileOperator(50 * 1204, 3);
        //divider.uploadFile(new File("dataNode/src/main/resources/uploadTest/test.png"));
        divider.downloadFile("test.png", "dataNode/src/main/resources/downloadTest/saturn.png");
    }
}
