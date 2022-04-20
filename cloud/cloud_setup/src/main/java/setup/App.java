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
    private static final String MY_YC_CENTRAL1_B_SUBNET_ID = "<subnet-id>";
    private static final String YC_STANDARD_IMAGES = "standard-images";
    private static final String YC_UBUNTU_IMAGE_FAMILY = "ubuntu-1804";

    private App() {
    }

    public static void setupNet(ServiceFactory factory, String networkName) throws Exception {
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
        Map<Zone, String> zoneToCidr = new HashMap<>();
        zoneToCidr.put(Zone.RU_CENTRAL1_A, "192.168.0.0/24");
        zoneToCidr.put(Zone.RU_CENTRAL1_B, "192.168.1.0/24");
        zoneToCidr.put(Zone.RU_CENTRAL1_C, "192.168.2.0/24");

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
    public static void main(String[] args) {

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
        // Configuration
        ServiceFactory factory = ServiceFactory.builder()
                .credentialProvider(Auth.oauthTokenBuilder().fromEnv("YC_OAUTH"))
                .requestTimeout(Duration.ofMinutes(1))
                .build();
        System.out.println("Hello World!");
    }
}
