package setup;

import yandex.cloud.api.compute.v1.ImageOuterClass.Image;
import yandex.cloud.api.compute.v1.ImageServiceGrpc;
import yandex.cloud.api.compute.v1.ImageServiceGrpc.ImageServiceBlockingStub;
import yandex.cloud.api.compute.v1.ImageServiceOuterClass.CreateImageMetadata;
import yandex.cloud.api.compute.v1.ImageServiceOuterClass.CreateImageRequest;
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

import yandex.cloud.sdk.Platform;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.utils.OperationTimeoutException;
import yandex.cloud.sdk.utils.OperationUtils;
import yandex.cloud.api.operation.OperationOuterClass.Operation;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.api.vpc.v1.NetworkServiceGrpc;
import yandex.cloud.api.vpc.v1.NetworkServiceGrpc.NetworkServiceBlockingStub;
import yandex.cloud.api.vpc.v1.NetworkServiceOuterClass.ListNetworkSubnetsRequest;
import yandex.cloud.api.vpc.v1.NetworkServiceOuterClass.ListNetworkSubnetsResponse;
import yandex.cloud.api.vpc.v1.SubnetOuterClass.Subnet;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;

public class Instances {
    private static String myFolderId;
    private static String imageFolderId;
    private static String familyId;
    public Instances(String myFolderId_, String imageFolderId_, String familyId_) {
        myFolderId = myFolderId_;
        imageFolderId = imageFolderId_;
        familyId = familyId_;
    }

    public void createImage(ServiceFactory factory, String family, String diskId) {
        OperationServiceBlockingStub operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        ImageServiceBlockingStub imageService = factory.create(ImageServiceBlockingStub.class, ImageServiceGrpc::newBlockingStub);
        Operation operation = imageService.create(buildCreateImageRequest(family, diskId));
        String imageId = "";
        try {
            imageId = operation.getMetadata().unpack(CreateImageMetadata.class).getImageId();
        } catch (InvalidProtocolBufferException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        try {
            OperationUtils.wait(operationService, operation, Duration.ofMinutes(5));
        } catch (OperationTimeoutException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println(String.format("Created with id %s", imageId));
    }

    private CreateImageRequest buildCreateImageRequest(String family, String diskId) {
        return CreateImageRequest.newBuilder()
                .setFolderId(myFolderId)
                .setFamily(family)
                .setDiskId(diskId)
                .build();
    }

    public void setupInstances(ServiceFactory factory, int instancesNum, String networkId, String imageId, String[] zones, long memory, long disk) {
        // Configuration
        InstanceServiceBlockingStub instanceService = factory.create(InstanceServiceBlockingStub.class, InstanceServiceGrpc::newBlockingStub);
        OperationServiceBlockingStub operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        ImageServiceBlockingStub imageService = factory.create(ImageServiceBlockingStub.class, ImageServiceGrpc::newBlockingStub);

        if (imageId == null) {
            Image image = imageService.getLatestByFamily(buildGetLatestByFamilyRequest());
            imageId = image.getId();
        }

        NetworkServiceBlockingStub networkService = factory.create(NetworkServiceBlockingStub.class, NetworkServiceGrpc::newBlockingStub);
        ListNetworkSubnetsResponse subnets = networkService.listSubnets(buildListNetworkSubnetsRequest(networkId));
        Map<String, Subnet> zoneToSubnet = new HashMap<>();
        for (Subnet subnet : subnets.getSubnetsList()) {
            zoneToSubnet.put(subnet.getZoneId(), subnet);
        }
        int zone_ind = 0;
        for (int i = 0; i < instancesNum; i++, zone_ind = (zone_ind + 1) % zones.length) {
            // Create instance
            Subnet subnet = zoneToSubnet.get(zones[zone_ind]);
            Operation createOperation = instanceService.create(buildCreateInstanceRequest(imageId, i, subnet.getZoneId(), subnet.getId(), memory, disk));
            System.out.println("Create instance request sent");

            // Wait for instance creation
            String instanceId = "";
            try {
                instanceId = createOperation.getMetadata().unpack(CreateInstanceMetadata.class).getInstanceId();
            } catch (InvalidProtocolBufferException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(1);
            }
            try {
                OperationUtils.wait(operationService, createOperation, Duration.ofMinutes(5));
            } catch (OperationTimeoutException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(1);
            }
            System.out.println(String.format("Created with id %s", instanceId));
        }
    }

    public void deleteInstances(ServiceFactory factory) {
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
            try {
                OperationUtils.wait(operationService, deleteOperation, Duration.ofMinutes(1));
            } catch (OperationTimeoutException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println(String.format("Deleted instance %s", instance.getId()));
        }
    }

    private static GetImageLatestByFamilyRequest buildGetLatestByFamilyRequest() {
        return GetImageLatestByFamilyRequest.newBuilder()
                .setFolderId(imageFolderId)
                .setFamily(familyId)
                .build();
    }

    private static CreateInstanceRequest buildCreateInstanceRequest(String imageId, int num, String zoneId, String subnetId, long memory, long disk) { // Gb
        return CreateInstanceRequest.newBuilder()
                .setFolderId(myFolderId)
                .setName("ubuntu" + String.valueOf(num))
                .setZoneId(zoneId)
                .setPlatformId(Platform.STANDARD_V2.getId())
                .setResourcesSpec(ResourcesSpec.newBuilder().setCores(2).setCoreFraction(5).setMemory(memory * 1024 * 1024 * 1024))
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
        return ListInstancesRequest.newBuilder().setFolderId(myFolderId).build();
    }

    private static DeleteInstanceRequest buildDeleteInstanceRequest(String instanceId) {
        return DeleteInstanceRequest.newBuilder().setInstanceId(instanceId).build();
    }

    private static ListNetworkSubnetsRequest buildListNetworkSubnetsRequest(String networkId) {
        return ListNetworkSubnetsRequest.newBuilder()
                .setNetworkId(networkId)
                .build();
    }
}
