package kafka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class AnalysisReport {
    final static String FILE_PATH = "result.csv";

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(FILE_PATH));
        String line = br.readLine();
        BidenAnalysis bidenAnalysis = new BidenAnalysis();
        TrumpAnalysis trumpAnalysis = new TrumpAnalysis();

        while ((line = br.readLine()) != null) {
            String[] cols = line.split(",");
            String key_word = cols[0];
            String sentiment = cols[4];

            if (key_word.equalsIgnoreCase("trump")) {
                trumpAnalysis.count++;
                trumpAnalysis.sentiments[Integer.parseInt(sentiment)]++;
            } else {
                bidenAnalysis.count++;
                bidenAnalysis.sentiments[Integer.parseInt(sentiment)]++;
            }
        }

        br.close();
        System.out.println("==========Report==========");
        System.out.println(String.format("There are %s tweets about Trump, %s are negative, %s are positive",
                trumpAnalysis.count,
                trumpAnalysis.sentiments[0]+trumpAnalysis.sentiments[1],
                trumpAnalysis.sentiments[3]+trumpAnalysis.sentiments[4]));
        System.out.println(String.format("There are %s tweets about Biden, %s are negative, %s are positive",
                bidenAnalysis.count,
                bidenAnalysis.sentiments[0]+bidenAnalysis.sentiments[1],
                bidenAnalysis.sentiments[3]+bidenAnalysis.sentiments[4]));
    }
}

class BidenAnalysis {
    int count = 0;
    int[] sentiments = new int[5];
}

class TrumpAnalysis {
    int count = 0;
    int[] sentiments = new int[5];
}
