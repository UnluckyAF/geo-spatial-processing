package setup;

import yandex.cloud.api.compute.v1.ImageOuterClass.Image;
import yandex.cloud.api.compute.v1.ImageServiceGrpc;
import yandex.cloud.api.compute.v1.ImageServiceGrpc.ImageServiceBlockingStub;
import yandex.cloud.api.compute.v1.ImageServiceOuterClass.GetImageLatestByFamilyRequest;
import yandex.cloud.api.compute.v1.InstanceOuterClass.Instance;
import yandex.cloud.api.compute.v1.InstanceServiceGrpc;
import yandex.cloud.api.compute.v1.InstanceServiceGrpc.InstanceServiceBlockingStub;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.AttachedDiskSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.AttachedDiskSpec.DiskSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.CreateInstanceMetadata;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.CreateInstanceRequest;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.DeleteInstanceRequest;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.ListInstancesRequest;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.NetworkInterfaceSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.PrimaryAddressSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.ResourcesSpec;
import yandex.cloud.api.operation.OperationOuterClass.Operation;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.api.vpc.v1.NetworkServiceOuterClass.CreateNetworkRequest;
import yandex.cloud.sdk.Platform;
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
import yandex.cloud.api.vpc.v1.SubnetOuterClass.Subnet;
import yandex.cloud.api.vpc.v1.SubnetServiceGrpc;
import yandex.cloud.api.vpc.v1.SubnetServiceGrpc.SubnetServiceBlockingStub;
import yandex.cloud.api.vpc.v1.SubnetServiceOuterClass.CreateSubnetMetadata;
import yandex.cloud.api.vpc.v1.SubnetServiceOuterClass.CreateSubnetRequest;
import yandex.cloud.api.vpc.v1.SubnetServiceOuterClass.DeleteSubnetMetadata;
import yandex.cloud.api.vpc.v1.SubnetServiceOuterClass.DeleteSubnetRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 */
public final class App {
    private static final String MY_YC_FOLDER_ID = "b1ga6r1fob64eg80duhg";
    private static final String YC_STANDARD_IMAGES = "standard-images";
    private static final String YC_UBUNTU_IMAGE_FAMILY = "ubuntu-1804";

    private App() {
    }

    public static void setupNet(ServiceFactory factory, String networkName, Map<Zone, String> zoneToCidr) throws Exception {
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

        // IPv4 CIDR for every availability zone
        // Map<Zone, String> zoneToCidr = new HashMap<>();
        // zoneToCidr.put(Zone.RU_CENTRAL1_A, commonCidr);
        // zoneToCidr.put(Zone.RU_CENTRAL1_B, commonCidr); // "192.168.1.0/24"
        // zoneToCidr.put(Zone.RU_CENTRAL1_C, commonCidr);

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

    public static void setupInstances(ServiceFactory factory, int instancesNum, String networkId, String[] zones, long memory, long disk) {
        // Configuration
        InstanceServiceBlockingStub instanceService = factory.create(InstanceServiceBlockingStub.class, InstanceServiceGrpc::newBlockingStub);
        OperationServiceBlockingStub operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        ImageServiceBlockingStub imageService = factory.create(ImageServiceBlockingStub.class, ImageServiceGrpc::newBlockingStub);

        // Get latest Ubuntu 18 image
        Image image = imageService.getLatestByFamily(buildGetLatestByFamilyRequest());

        NetworkServiceBlockingStub networkService = factory.create(NetworkServiceBlockingStub.class, NetworkServiceGrpc::newBlockingStub);
        ListNetworkSubnetsResponse subnets = networkService.listSubnets(buildListNetworkSubnetsRequest(networkId));
        Map<String, Subnet> zoneToSubnet = new HashMap<>();
        for (Subnet subnet : subnets.getSubnetsList()) {
            zoneToSubnet.put(subnet.getZoneId(), subnet);
        }
        List<Subnet> subnetsList = subnets.getSubnetsList();
        int zone_ind = 0;
        for (int i = 0; i < instancesNum; i++, zone_ind = (zone_ind + 1) % zones.length) {
            // Create instance
            Subnet subnet = zoneToSubnet.get(zones[zone_ind]);
            Operation createOperation = instanceService.create(buildCreateInstanceRequest(image.getId(), subnet.getZoneId(), subnet.getId(), memory, disk));
            System.out.println("Create instance request sent");

            // Wait for instance creation
            String instanceId = createOperation.getMetadata().unpack(CreateInstanceMetadata.class).getInstanceId();
            OperationUtils.wait(operationService, createOperation, Duration.ofMinutes(5));
            System.out.println(String.format("Created with id %s", instanceId));
        }
    }

    public static void deleteInstances(ServiceFactory factory) {
        InstanceServiceBlockingStub instanceService = factory.create(InstanceServiceBlockingStub.class, InstanceServiceGrpc::newBlockingStub);
        OperationServiceBlockingStub operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        // List instances in the folder
        List<Instance> instances = instanceService.list(buildListInstancesRequest()).getInstancesList();
        instances.forEach(System.out::println);
        System.out.println("Listed instances");

        for (Instance instance : instances) {
            // Delete created instance
            Operation deleteOperation = instanceService.delete(buildDeleteInstanceRequest(instance.getId()));
            System.out.println("Delete instance request sent");

            // Wait for instance deletion
            OperationUtils.wait(operationService, deleteOperation, Duration.ofMinutes(1));
            System.out.println(String.format("Deleted instance %s", instance.getId()));
        }
    }

    private static GetImageLatestByFamilyRequest buildGetLatestByFamilyRequest() {
        return GetImageLatestByFamilyRequest.newBuilder()
                .setFolderId(YC_STANDARD_IMAGES)
                .setFamily(YC_UBUNTU_IMAGE_FAMILY)
                .build();
    }

    private static CreateInstanceRequest buildCreateInstanceRequest(String imageId, String zoneId, String subnetId, long memory, long disk) { // Gb
        return CreateInstanceRequest.newBuilder()
                .setFolderId(MY_YC_FOLDER_ID)
                .setName("ubuntu")
                .setZoneId(zoneId)
                .setPlatformId(Platform.STANDARD_V2.getId())
                .setResourcesSpec(ResourcesSpec.newBuilder().setCores(1).setCoreFraction(5).setMemory(memory * 1024 * 1024 * 1024))
                .setBootDiskSpec(AttachedDiskSpec.newBuilder()
                        .setDiskSpec(DiskSpec.newBuilder()
                                .setImageId(imageId)
                                .setSize(disk * 1024 * 1024 * 1024)))
                .addNetworkInterfaceSpecs(NetworkInterfaceSpec.newBuilder()
                        .setSubnetId(subnetId)
                        .setPrimaryV4AddressSpec(PrimaryAddressSpec.getDefaultInstance())
                ).build();
    }

    private static ListInstancesRequest buildListInstancesRequest() {
        return ListInstancesRequest.newBuilder().setFolderId(MY_YC_FOLDER_ID).build();
    }

    private static DeleteInstanceRequest buildDeleteInstanceRequest(String instanceId) {
        return DeleteInstanceRequest.newBuilder().setInstanceId(instanceId).build();
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        // Configuration
        ServiceFactory factory = ServiceFactory.builder()
                .credentialProvider(Auth.oauthTokenBuilder().fromEnv("YC_OAUTH"))
                .requestTimeout(Duration.ofMinutes(1))
                .build();
        System.out.println("Hello World!");
    }
}
