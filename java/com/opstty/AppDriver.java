package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtdriver", DistrictDriver.class,
                    "A map/reduce program that lists districts containing trees.");
            programDriver.addClass("speciesdriver", SpeciesDriver.class,
                    "A map/reduce program that lists all existing species of trees.");
            programDriver.addClass("treekinddriver", TreeKindDriver.class,
                    "A map/reduce program that counts the number of trees of each kind.");
            programDriver.addClass("maxheightdriver", MaxHeightDriver.class,
                    "A map/reduce program that finds the maximum height of each kind of tree.");
            programDriver.addClass("sortheightdriver", SortHeightDriver.class,
                    "A map/reduce program that sorts trees by height.");
            programDriver.addClass("oldesttreedriver", OldestTreeDriver.class,
                    "A map/reduce program that finds the district containing the oldest tree.");
            programDriver.addClass("treecountdriver", TreeCountDriver.class,
                    "A map/reduce program that counts trees per district.");
            programDriver.addClass("maxtreedriver", MaxTreeDriver.class,
                    "A map/reduce program that finds the district with the most trees.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
