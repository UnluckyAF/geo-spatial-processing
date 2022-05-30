package setup;

import yandex.cloud.api.operation.OperationOuterClass.Operation;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.api.vpc.v1.NetworkServiceOuterClass.CreateNetworkRequest;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.Zone;
import yandex.cloud.sdk.auth.Auth;
import yandex.cloud.sdk.utils.OperationUtils;
import yandex.cloud.api.vpc.v1.NetworkServiceGrpc;
import yandex.cloud.api.vpc.v1.NetworkServiceGrpc.NetworkServiceBlockingStub;
import yandex.cloud.api.vpc.v1.NetworkServiceOuterClass.CreateNetworkMetadata;
import yandex.cloud.api.vpc.v1.NetworkServiceOuterClass.DeleteNetworkRequest;
import yandex.cloud.api.vpc.v1.NetworkServiceOuterClass.ListNetworkSubnetsRequest;
import yandex.cloud.api.vpc.v1.NetworkServiceOuterClass.ListNetworkSubnetsResponse;
import yandex.cloud.api.vpc.v1.SubnetServiceGrpc;
import yandex.cloud.api.vpc.v1.SubnetServiceGrpc.SubnetServiceBlockingStub;
import yandex.cloud.api.vpc.v1.SubnetServiceOuterClass.CreateSubnetMetadata;
import yandex.cloud.api.vpc.v1.SubnetServiceOuterClass.CreateSubnetRequest;
import yandex.cloud.api.vpc.v1.SubnetServiceOuterClass.DeleteSubnetMetadata;
import yandex.cloud.api.vpc.v1.SubnetServiceOuterClass.DeleteSubnetRequest;

import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 */
public final class App {
    private static final String MY_YC_FOLDER_ID = "b1g32dram6ql673m3pn7";
    private static final String YC_STANDARD_IMAGES = "standard-images";
    private static final String YC_UBUNTU_IMAGE_FAMILY = "ubuntu-1804";

    private App() {
    }

    public static String setupNet(ServiceFactory factory, String networkName, Map<Zone, String> zoneToCidr) throws Exception {
        NetworkServiceBlockingStub networkService = factory.create(NetworkServiceBlockingStub.class, NetworkServiceGrpc::newBlockingStub);
        SubnetServiceBlockingStub subnetService = factory.create(SubnetServiceBlockingStub.class, SubnetServiceGrpc::newBlockingStub);
        OperationServiceBlockingStub operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);

        // Create network
        Operation createOperation = networkService.create(buildCreateNetworkRequest(networkName));
        System.out.println("Create network request sent");

        // Wait for network creation
        String networkId = createOperation.getMetadata().unpack(CreateNetworkMetadata.class).getNetworkId();
        OperationUtils.wait(operationService, createOperation, Duration.ofMinutes(1));
        System.out.println(String.format("Created network with id %s", networkId));

        // Create subnets in all 3 availability zones
        List<Operation> createSubnetOperations = new ArrayList<>();
        zoneToCidr.forEach((zone, cidr) ->
                createSubnetOperations.add(subnetService.create(buildCreateSubnetRequest(networkId, zone, cidr))));

        // Wait for subnet creation
        for (Operation operation : createSubnetOperations) {
            String subnetId = operation.getMetadata().unpack(CreateSubnetMetadata.class).getSubnetId();
            OperationUtils.wait(operationService, operation, Duration.ofMinutes(1));
            System.out.println(String.format("Created subnet %s", subnetId));
        }
        return networkId;
    }

    public static void deleteNet(ServiceFactory factory, String networkId) throws Exception {
        NetworkServiceBlockingStub networkService = factory.create(NetworkServiceBlockingStub.class, NetworkServiceGrpc::newBlockingStub);
        SubnetServiceBlockingStub subnetService = factory.create(SubnetServiceBlockingStub.class, SubnetServiceGrpc::newBlockingStub);
        OperationServiceBlockingStub operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        // List subnets in created network
        ListNetworkSubnetsResponse subnets = networkService.listSubnets(buildListNetworkSubnetsRequest(networkId));

        // Delete all subnets
        List<Operation> deleteSubnetOperations = new ArrayList<>();
        subnets.getSubnetsList().forEach(subnet ->
                deleteSubnetOperations.add(subnetService.delete(buildDeleteSubnetRequest(subnet.getId()))));

        // Wait for subnet deletion
        for (Operation operation : deleteSubnetOperations) {
            String subnetId = operation.getMetadata().unpack(DeleteSubnetMetadata.class).getSubnetId();
            OperationUtils.wait(operationService, operation, Duration.ofMinutes(1));
            System.out.println(String.format("Deleted subnet %s", subnetId));
        }

        // Delete created network
        Operation deleteOperation = networkService.delete(buildDeleteNetworkRequest(networkId));
        System.out.println("Delete network request sent");

        // Wait for network deletion
        OperationUtils.wait(operationService, deleteOperation, Duration.ofMinutes(1));
        System.out.println(String.format("Deleted network %s", networkId));
    }

    private static CreateNetworkRequest buildCreateNetworkRequest(String networkName) {
        if (networkName.length() == 0) {
            networkName = "network1";
        }
        return CreateNetworkRequest.newBuilder()
                .setName(networkName)
                .setFolderId(MY_YC_FOLDER_ID)
                .build();
    }

    private static CreateSubnetRequest buildCreateSubnetRequest(String networkId, Zone zone, String v4Cidr) {
        return CreateSubnetRequest.newBuilder()
                .setFolderId(MY_YC_FOLDER_ID)
                .setName("subnet-" + zone.getId())
                .setNetworkId(networkId)
                .setZoneId(zone.getId())
                .addV4CidrBlocks(v4Cidr)
                .build();
    }

    private static ListNetworkSubnetsRequest buildListNetworkSubnetsRequest(String networkId) {
        return ListNetworkSubnetsRequest.newBuilder()
                .setNetworkId(networkId)
                .build();
    }

    private static DeleteSubnetRequest buildDeleteSubnetRequest(String subnetId) {
        return DeleteSubnetRequest.newBuilder()
                .setSubnetId(subnetId)
                .build();
    }

    private static DeleteNetworkRequest buildDeleteNetworkRequest(String networkId) {
        return DeleteNetworkRequest.newBuilder()
                .setNetworkId(networkId)
                .build();
    }
    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        Yaml yaml = new Yaml();
        File initialFile = new File("/Users/barukhov/geo_spatial_data/hehe/cloud/cloud_setup/test.yaml");
        InputStream targetStream = null;
        try {
            targetStream = new FileInputStream(initialFile);
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        Map<String, Object> obj = yaml.load(targetStream);
        System.out.println(obj);
        Options options = new Options();

        Option action = new Option("a", "action", true, "action to do");
        action.setRequired(true);
        options.addOption(action);

        Option networkIdOption = new Option("network_id", true, "network id");
        options.addOption(networkIdOption);

        Option imageFamilyOption = new Option("image_family", true, "name of the family for the image which is going to be created");
        options.addOption(imageFamilyOption);
        Option imageDiskIdOption = new Option("image_disk",  true, "disk id of disk from which image is going to be created");
        options.addOption(imageDiskIdOption);

        Option withVpnOption = new Option("v", "with_vpn",  false, "create addtitional vpn instance");
        options.addOption(withVpnOption);
        Option vpnZoneOption = new Option("vzone", "vpn_zone",  true, "subnet zone of vpn instance");
        options.addOption(vpnZoneOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

        String actionString = cmd.getOptionValue("action");
        String network = (String) obj.get("network");
        List<String> cidrString = (List<String>) obj.get("cidr");
        String instancesNum = Integer.toString((Integer) obj.get("instances"));
        List<String> zonesStrings = (List<String>) obj.get("zones");
        String memoryString = Integer.toString((Integer) obj.get("memory"));
        String diskString = Integer.toString((Integer) obj.get("disk"));
        String networkId = cmd.getOptionValue("network_id");
        String imageId = (String) obj.getOrDefault("image", null);
        String imageDiskId = cmd.getOptionValue("image_disk");
        String imageFamily = (String) obj.getOrDefault("image_family", "my-family");

        boolean withVpn = cmd.hasOption("with_vpn");
        String vpnZone = Zone.RU_CENTRAL1_A.getId();
        if (cmd.hasOption("vpn_zone")) {
            vpnZone = cmd.getOptionValue("vpn_zone");
        }

        // Configuration
        ServiceFactory factory = ServiceFactory.builder()
                .credentialProvider(Auth.oauthTokenBuilder().fromEnv("YC_OAUTH"))
                .requestTimeout(Duration.ofMinutes(1))
                .build();
        System.out.println("Hello World!");

        Instances inst = new Instances(MY_YC_FOLDER_ID, YC_STANDARD_IMAGES, YC_UBUNTU_IMAGE_FAMILY, "cloud_config.yaml", "/Users/barukhov/.ssh/cloud.pub");
        switch (actionString) {
            case ("setup"):
                // IPv4 CIDR for every availability zone
                Map<Zone, String> zoneToCidr = new HashMap<>();
                zoneToCidr.put(Zone.RU_CENTRAL1_A, cidrString.get(0));
                zoneToCidr.put(Zone.RU_CENTRAL1_B, cidrString.get(1));
                zoneToCidr.put(Zone.RU_CENTRAL1_C, cidrString.get(2));

                if (networkId == null) {
                    try {
                            networkId = setupNet(factory, network, zoneToCidr);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                inst.setupInstances(
                    factory,
                    Integer.parseInt(instancesNum),
                    networkId,
                    imageId,
                    zonesStrings,
                    Integer.parseInt(memoryString),
                    Integer.parseInt(diskString),
                    withVpn,
                    vpnZone
                );
                break;
            case ("delete"):
                if (networkId == null) {
                    System.err.println("No network id provided");
                    System.exit(1);
                }

                inst.deleteInstances(factory);
                try {
                    deleteNet(factory, networkId);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                break;
            case ("create"):
                inst.createImage(factory, imageFamily, imageDiskId);
                break;
            default:
                System.out.println("Unknown action");

        }
        System.out.println("Finish");
    }
}
