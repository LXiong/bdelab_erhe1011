package de.hska.iwi.bdelab.batchstore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import de.hska.iwi.bdelab.schema.*;
import manning.tap.DataPailStructure;

import org.apache.hadoop.fs.FileSystem;

import com.backtype.hadoop.pail.Pail;

public class Batchloader {

    // ...
	private Pail<Data> tmpPail;
	private Pail<Data> masterPail;
	Pail<Data>.TypedRecordOutputStream os;

    private void readPageviewsAsStream() {
        try {
            URI uri = Batchloader.class.getClassLoader().getResource("pageviews.txt").toURI();
            try (Stream<String> stream = Files.lines(Paths.get(uri))) {
                stream.forEach(line -> {
					try {
						writeToPail(getDatafromString(line));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (URISyntaxException e1) {
            e1.printStackTrace();
        }
    }

    private Data getDatafromString(String pageview) {
        Data result = null;
        // change this to "true" if you want to work
        // on the local machines' file system instead of hdfs
        StringTokenizer tokenizer = new StringTokenizer(pageview);
        String ip = tokenizer.nextToken();
        String url = tokenizer.nextToken();
        String time = tokenizer.nextToken();

        System.out.println(ip + " " + url + " " + time);

        // ... create Data
        UserID uid1 = new UserID();
		uid1.set_user_id(ip);

		PageID pg1 = new PageID();
		pg1.set_url(url);

		PageViewEdge pve1 = new PageViewEdge();
		pve1.set_person(uid1);
		pve1.set_page(pg1);
		pve1.set_nonce(Integer.parseInt(time));

		DataUnit du1 = new DataUnit();
		du1.set_page_view(pve1);

		Pedigree pd1 = new Pedigree();
		pd1.set_true_as_of_secs((int) (System.currentTimeMillis() / 1000));
		result = new Data();
		result.set_dataunit(du1);
		result.set_pedigree(pd1);

        return result;
    }

    private void writeToPail(Data data) throws IOException {
        // ...
    	os.writeObjects(data);
    }

    private void importPageviews() {

        // change this to "true" if you want to work
        // on the local machines' file system instead of hdfs
        boolean LOCAL = false;

        try {
            // set up filesystem
            FileSystem fs = FileUtils.getFs(LOCAL);

            // prepare temporary pail folder
            String newPath = FileUtils.prepareNewFactsPath(true, LOCAL);

            // master pail goes to permanent fact store
            String masterPath = FileUtils.prepareMasterFactsPath(false, LOCAL);

            // set up new pail and a stream
            // ...
            tmpPail = Pail.create(fs, newPath, new DataPailStructure());
            os = tmpPail.openWrite();
            // write facts to new pail
            readPageviewsAsStream();
            
            os.close();
            // set up master pail and absorb new pail
            // ...
            masterPail = Pail.create(fs, masterPath, new DataPailStructure(), false);
            masterPail.absorb(tmpPail);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Batchloader loader = new Batchloader();
        loader.importPageviews();
    }
}