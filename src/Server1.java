import java.io.*;
import java.net.Socket;
import java.util.Random;


public class Server1 {

    static int FILE_SIZE = 100;
    static int NOF_FILES = 10;
    static int DELAY_MIN = 5;

    public static void main(String[] args) {

        int i = 0;
        int fileCounter = NOF_FILES;
        StringBuilder stringBuilder = new StringBuilder();
        String header;
        String headerFilePath = "./header.txt"; // header file
        String csvPath = "./people-2000000.csv";
        String newCsvFile = null;
        String[] rows = new String[2000000];
        Socket socket = null;


        try (BufferedReader br = new BufferedReader(new FileReader(headerFilePath))) {  // header alanları okundu
            String line;
            while ((line = br.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(',');
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1); //son virgül silindi
        } catch (IOException e) {
            e.printStackTrace();
        }

        header = stringBuilder.toString();

        //csv dosyası oluşturulurken gerekecek veriler örnek csv dosyasından satır satır alındı
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvPath));
            String line = br.readLine();  //header satırı atlandı
            while ((line = br.readLine()) != null) {
                rows[i] = line;
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (fileCounter > 0) {

            StringBuilder newFileName = new StringBuilder();
            newFileName.append("newCsv");
            newFileName.append(NOF_FILES - fileCounter + 1);
            newFileName.append(".csv");
            newCsvFile = newFileName.toString();
            long preferredFileSizeInBytes = 1024 * 1024 * FILE_SIZE; //100 mb

            //yeni csv dosyası oluşturuldu
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(newCsvFile))) {

                //header satırı yazıldı
                writer.write(header);
                writer.newLine();
                long currentFileSize = header.getBytes().length + System.lineSeparator().getBytes().length;
                Random random = new Random();
                int rowIdx = random.nextInt(2000000);

                //örnek veriler rastgele satırlar seçilerek yeni dosyaya yazıldı
                while (currentFileSize < preferredFileSizeInBytes) {
                    writer.write(rows[rowIdx]);
                    writer.newLine();
                    writer.flush();

                    // dosya güncellenip boyutu kontrol edildi. 100 mb boyuta ulaştığında dosyaya yazdırma durdu
                    currentFileSize += rows[rowIdx].getBytes().length + System.lineSeparator().getBytes().length;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // program 5 dakika boyunca durduruldu
            try {
                Thread.sleep(DELAY_MIN*60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //yaratılan dosyalar server2 ye gönderildi
            try{
                socket = new Socket("localhost", 1234);

                File file = new File(newCsvFile);
                byte[] fileBytes = new byte[(int) file.length()];

                try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
                    bis.read(fileBytes, 0, fileBytes.length);
                }

                OutputStream os = socket.getOutputStream();
                DataOutputStream dos = new DataOutputStream(os);

                dos.write(file.getName().getBytes());
                dos.write(fileBytes, 0, fileBytes.length);
                dos.flush();
                System.out.println("File sent to the server.");

            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                try{
                    if(socket != null) {
                        socket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            fileCounter--;
        }
    }
}

/*
    mport java.io.FileWriter;
        import java.io.IOException;
public class CSVFileWriter {
    public static void main(String[] args) {
        // Sample data to be written to the CSV file
        String[] headers = { "Name", "Age", "City" };
        String[][] data = {
                { "John Doe", "30", "New York" },
                { "Jane Smith", "25", "Los Angeles" },
                { "Bob Johnson", "35", "Chicago" }
        };

        String csvFile = "output.csv";

        writeDataToCSV(csvFile, headers, data);
    }

    public static void writeDataToCSV(String fileName, String[] headers, String[][] data) {
        try (FileWriter writer = new FileWriter(fileName)) {
            // Write headers
            for (int i = 0; i < headers.length; i++) {
                writer.append(headers[i]);
                if (i != headers.length - 1) {
                    writer.append(",");
                }
            }
            writer.append("\n");

            // Write data rows
            for (String[] row : data) {
                for (int i = 0; i < row.length; i++) {
                    writer.append(row[i]);
                    if (i != row.length - 1) {
                        writer.append(",");
                    }
                }
                writer.append("\n");
            }

            System.out.println("CSV file created successfully: " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
*/
