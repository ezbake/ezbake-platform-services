/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.quarantine.client;

import ezbake.base.thrift.Visibility;
import ezbake.configuration.DirectoryConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.quarantine.thrift.*;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by nkhan on 4/15/14.
 */
public class QuarantineCmdLineTool {

    private static final Logger logger = LoggerFactory.getLogger(QuarantineCmdLineTool.class);

    @Option(name="-s", aliases = "--securityId",
            usage="The security ID to use for the Quarantine Client. This will override whatever is read in from EzConfiguration", required = false)
    private String securityId;

    private int id = 0;

    public void run() throws IOException, EzConfigurationLoaderException {
        Properties props = new EzConfiguration().getProperties();

        if (!props.containsKey(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING)) {
            throw new RuntimeException("No EzConfiguration properties loaded. Make sure to set the " + DirectoryConfigurationLoader.EZCONFIGURATION_PROPERTY + " system property");
        }

        if (securityId != null) {
            props.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, securityId);
        }

        if (new EzBakeApplicationConfigurationHelper(props).getSecurityID() == null) {
            throw new RuntimeException("No security ID set. Verify that security ID is set in properties or provide security ID with -s parameter");
        }

        QuarantineClient client = new QuarantineClient(props);

        boolean optionsDisplayed;

        String input = "";
        String selectedPipeline = "";
        Scanner sc = new Scanner(System.in);

        while(!input.equals("exit")) {
            System.out.println("Enter pipeline name");
            input = sc.nextLine();
            selectedPipeline = input;
            optionsDisplayed = false;
            while(true) {

                //All items are listed for the selected pipe along with options
                List<QuarantineResult> items = null;

                //Selected pipeline is used for further commands
                //until explicitly changed by the user
                if (!optionsDisplayed) {
                    items = loadItems(selectedPipeline, client);
                    displayPipelineOptions(input);
                    optionsDisplayed = true;
                }
                //See which option the user picked
                input = sc.nextLine();

                logger.info("input: " + input);

                if(input.equalsIgnoreCase("V")){
                    items = loadItems(selectedPipeline, client);
                    if(items !=null)
                      printItems(items);
                    else
                      System.out.println("No items to display");
                    //User selected update status
                }else if (input.equalsIgnoreCase("U")) {
                    System.out.println("Enter item id: ");
                    input = sc.nextLine();
                    items = loadItems(selectedPipeline, client);
                    //Update status command executed
                    QuarantineResult item = getItemForId(items, input);
                    if (item != null) {
                        //Item for the id exists
                        //Prompt user to enter a new valid status
                        ObjectStatus status = getValidStatus(sc);
                        if(status != null) {
                            //Update the status
                            client.updateStatus(item.id, status, "");
                            System.out.println("Status updated successfully");
                        } else {
                            System.out.println("invalid option, exiting...");
                            input = "exit";
                            break;
                        }
                    } else {
                        System.out.println("No item for id : " );
                    }
                } else if(input.equals("S")) {
                    //Send to quarantine option was selected
                    System.out.println("Adding metadata? y/n: ");
                    input = sc.nextLine();
                    AdditionalMetadata additionalMetadata = new AdditionalMetadata();
                    while (input.equals("y")) {
                        System.out.println("Enter key:");
                        String key = sc.nextLine();
                        System.out.println("Enter value:");
                        String value = sc.nextLine();
                        additionalMetadata.putToEntries(key, new MetadataEntry().setValue(value));

                        System.out.println("Adding metadata? y/n: ");
                        input = sc.nextLine();
                    }
                    handleSendToQuarantine(selectedPipeline, additionalMetadata, client);
                } else if(input.equals("C")) {
                    //Change pipeline option was selected
                    //break out of the inner loop
                    break;
                } else if(input.equals("T")) {
                    //Pump a bunch of data in
                    for (int i = 0; i < 10000; i++) {
                        handleSendToQuarantineSameError(selectedPipeline, client);
                    }
                } else if(input.equals("E")) {
                    //User executed exit command break
                    //out of inner loop and change input
                    //to exit ot break out of outer loop
                    input = "exit";
                    break;
                } else {
                    System.out.println("Invalid command");
                    displayPipelineOptions(input);
                }
            }
        }

        System.out.println("TERMINATED!");
        System.exit(1);

    }

    private void handleSendToQuarantineSameError(String pipeline, QuarantineClient client) throws IOException {

        HTMLDocument doc = new HTMLDocument(++id, "content: " + id,
                new Date().toString());
        System.out.println("Sending thrift data to quarantine. Type: " + doc.getClass().getSimpleName());
        client.sendObjectToQuarantine(pipeline, "test-pipe", doc, new Visibility().setFormalVisibility("U"), "error msg", null);
    }

    private void handleSendToQuarantine(String pipeline, AdditionalMetadata additionalMetadata, QuarantineClient client) throws IOException {

        HTMLDocument doc = new HTMLDocument(++id, "content: " + id,
                new Date().toString());
        System.out.println("Sending thrift data to quarantine. Type: " + doc.getClass().getSimpleName());
        client.sendObjectToQuarantine(pipeline, "test-pipe", doc, new Visibility().setFormalVisibility("U"), "error msg " + id, additionalMetadata.getEntriesSize() > 0 ? additionalMetadata : null);
    }

    /**
     * Returns a status based on user input
     * @param sc scanner to read the user input from
     * @return a valid status or null if user chose to exit
     */
    private ObjectStatus getValidStatus(Scanner sc) {
        while(true) {
            System.out.println("Enter new status: \n" +
                    "A: Approved \n" +
                    "R: Reject \n" +
                    "E: Exit/Cancel status update");
            String input = sc.nextLine();

            if (input.equalsIgnoreCase("A")) {
                return ObjectStatus.APPROVED_FOR_REINGEST;
            } else if (input.equalsIgnoreCase("R")) {
                return ObjectStatus.ARCHIVED;
            } else if (input.equalsIgnoreCase("E")){
                return null;
            } else {
                System.out.print("Invalid option.");
            }
        }
    }

    /**
     * Displays options for the selected pipeline
     * @param input the name of the selected pipeline
     */
    private void displayPipelineOptions(String input) {
        System.out.println("Pipeline selected " + input);
        System.out.println("Options: ");
        System.out.println("C: Change Pipeline\n" +
                "V: View items\n" +
                "S: Send an item to quarantine\n" +
                "U: Update status for an item \n" +
                "T: Add a ton of data\n" +
                "E: exit\n");
    }

    /**
     * Searches through the list of items to find the item
     * with the requested id
     * @param items list of items to search through
     * @param reqId the id to match
     * @return QuarantineResult if it is found for the provided id, null otherwise
     */
    private QuarantineResult getItemForId(List<QuarantineResult> items, String reqId) {
        for(QuarantineResult item: items){
            if(item.id.equals(reqId))
                return item;
        }

        return null;
    }

    private List<QuarantineResult> loadItems(String pipeline, QuarantineClient client){
        try {
            Set<ObjectStatus> statuses = new HashSet<>();
            statuses.add(ObjectStatus.APPROVED_FOR_REINGEST);
            statuses.add(ObjectStatus.QUARANTINED);
            statuses.add(ObjectStatus.ARCHIVED);
            List<QuarantineResult> items = client.getObjectsForPipeline(pipeline, statuses, 0, (short)1000);
            return items;
        } catch (Exception e) {
            System.out.println("Error retrieving items " + e.getMessage());
            return null;
        }
    }


    /**
     * Goes through the list of quarantine results items and prints each item
     * @param items list of items to print
     */
    private void printItems(List<QuarantineResult> items) {
        for(QuarantineResult item : items){
            printItem(item);
        }
    }

    /**
     * Pretty prints a single item
     * @param item the item to print
     */
    private void printItem(QuarantineResult item) {
        StringBuilder sb = new StringBuilder();
        sb.append("id: ").append(item.id).append('\n');
        sb.append("status: ").append(getStatus(item.status)).append('\n');
        sb.append("----Event info: \n").append(getEventInfo(item.events)).append('\n');
        System.out.println(sb.toString());
    }

    /**
     * Returns a string for pretty printing events
     * @param events the list of events to extract information from
     * @return formatted string
     */
    private String getEventInfo(List<QuarantineEvent> events) {
        StringBuilder sb  = new StringBuilder();
        Integer eventCounter = 1;

        for(QuarantineEvent event : events) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");

            sb.append("Event # ").append(eventCounter).append('\n');
            sb.append("timestamp: ").append(format.format(event.getTimestamp())).append('\n');
            sb.append("error msg: ").append(event.event).append('\n');
            eventCounter++;
        }
        return sb.toString();
    }

    /**
     * Returns string representation of status
     * @param status status to check
     * @return string representation of status
     */
    private String getStatus(ObjectStatus status) {
        switch (status){
            case QUARANTINED:
                return "QUARANTINED";
            case APPROVED_FOR_REINGEST:
                return "APPROVED_FOR_REINGEST";
            case ARCHIVED:
                return "ARCHIVED";
            default:
                return "";
        }
    }

    public static void main(String[] args){
        QuarantineCmdLineTool client = new QuarantineCmdLineTool();
        CmdLineParser parser = new CmdLineParser(client);

        try {
            parser.parseArgument(args);
            client.run();
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

    }

}
