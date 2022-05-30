package setup;

import yandex.cloud.api.compute.v1.ImageOuterClass.Image;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.ImageServiceGrpc;
import yandex.cloud.api.compute.v1.ImageServiceGrpc.ImageServiceBlockingStub;
import yandex.cloud.api.compute.v1.ImageServiceOuterClass.CreateImageMetadata;
import yandex.cloud.api.compute.v1.ImageServiceOuterClass.CreateImageRequest;
import yandex.cloud.api.compute.v1.ImageServiceOuterClass.GetImageLatestByFamilyRequest;
import yandex.cloud.api.compute.v1.InstanceOuterClass.Instance;
import yandex.cloud.api.compute.v1.InstanceOuterClass.IpVersion;
import yandex.cloud.api.compute.v1.InstanceServiceGrpc;
import yandex.cloud.api.compute.v1.DiskOuterClass.Disk;
import yandex.cloud.api.compute.v1.DiskServiceGrpc.DiskServiceBlockingStub;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass.DeleteDiskRequest;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass.ListDisksRequest;
import yandex.cloud.api.compute.v1.InstanceServiceGrpc.InstanceServiceBlockingStub;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.AttachedDiskSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.AttachedDiskSpec.DiskSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.NetworkInterfaceSpec.Builder;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.CreateInstanceMetadata;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.CreateInstanceRequest;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.DeleteInstanceRequest;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.ListInstancesRequest;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.NetworkInterfaceSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.OneToOneNatSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.PrimaryAddressSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.ResourcesSpec;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.UpdateInstanceMetadataMetadata;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.UpdateInstanceMetadataRequest;
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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;

public class Instances {
    private static final String VPN_FAMILY_ID = "openvpn";

    private static String myFolderId;
    private static String imageFolderId;
    private static String familyId;
    private String cloudConfig;
    private String sshKeys;
    public Instances(String myFolderId_, String imageFolderId_, String familyId_, String cloudConfigPath_, String sshKeysPath_) {
        myFolderId = myFolderId_;
        imageFolderId = imageFolderId_;
        familyId = familyId_;
        try {
            cloudConfig = readFile(cloudConfigPath_, StandardCharsets.UTF_8);
            sshKeys = readFile(sshKeysPath_, StandardCharsets.UTF_8);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
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

    public void setupInstances(
        ServiceFactory factory,
        int instancesNum,
        String networkId,
        String imageId,
        List<String> zones,
        long memory,
        long disk,
        boolean withVpn,
        String vpnZone
    ) {
        // Configuration
        InstanceServiceBlockingStub instanceService = factory.create(InstanceServiceBlockingStub.class, InstanceServiceGrpc::newBlockingStub);
        OperationServiceBlockingStub operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        ImageServiceBlockingStub imageService = factory.create(ImageServiceBlockingStub.class, ImageServiceGrpc::newBlockingStub);

        if (imageId == null) {
            Image image = imageService.getLatestByFamily(buildGetLatestByFamilyRequest(familyId));
            imageId = image.getId();
        }

        NetworkServiceBlockingStub networkService = factory.create(NetworkServiceBlockingStub.class, NetworkServiceGrpc::newBlockingStub);
        ListNetworkSubnetsResponse subnets = networkService.listSubnets(buildListNetworkSubnetsRequest(networkId));
        Map<String, Subnet> zoneToSubnet = new HashMap<>();
        String vpnSubnet = null;

        for (Subnet subnet : subnets.getSubnetsList()) {
            zoneToSubnet.put(subnet.getZoneId(), subnet);
            if (vpnSubnet == null && subnet.getZoneId().equals(vpnZone)) {
                vpnSubnet = subnet.getId();
            }
        }
        int zone_ind = 0;
        for (int i = 0; i < instancesNum; i++, zone_ind = (zone_ind + 1) % zones.size()) {
            // Create instance
            Subnet subnet = zoneToSubnet.get(zones.get(zone_ind));
            Operation createOperation = instanceService.create(buildCreateInstanceRequest(
                imageId,
                "myvm" + String.valueOf(i),
                subnet.getZoneId(),
                subnet.getId(),
                memory,
                disk,
                true //TODO: tmp for ssh
            ));
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

            //TODO: remove
            updateMetadata(instanceService, operationService, instanceId);
        }

        if (withVpn && vpnSubnet != null) {
            createVPNInstance(instanceService, operationService, imageService, vpnZone, vpnSubnet);
        }
    }

    private void createVPNInstance(
        InstanceServiceBlockingStub instanceService,
        OperationServiceBlockingStub operationService,
        ImageServiceBlockingStub imageService,
        String zoneId,
        String subnetId
    ) {
        Image image = imageService.getLatestByFamily(buildGetLatestByFamilyRequest(VPN_FAMILY_ID));
        String imageId = image.getId();

        Operation createOperation = instanceService.create(buildCreateInstanceRequest(
            imageId,
            "vpn0",
            zoneId,
            subnetId,
            2,
            10,
            true
        ));
        System.out.println("Create vpn instance request sent");

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

        updateMetadata(instanceService, operationService, instanceId);
    }

    private void updateMetadata(InstanceServiceBlockingStub instanceService, OperationServiceBlockingStub operationService, String instanceId) {
        Operation updateMetaOperation = instanceService.updateMetadata(buildUpdateMetadateRequest(instanceId));

        try {
            OperationUtils.wait(operationService, updateMetaOperation, Duration.ofMinutes(5));
        } catch (OperationTimeoutException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println(String.format("Added metadata with id %s", instanceId));
    }

    private UpdateInstanceMetadataRequest buildUpdateMetadateRequest(String instanceId) {
        return UpdateInstanceMetadataRequest.newBuilder()
                .setInstanceId(instanceId)
                .putUpsert("user-data", cloudConfig)
                .putUpsert("ssh-keys", "ubuntu:"+sshKeys)
                .build();
    }

    public void deleteInstances(ServiceFactory factory) {
        InstanceServiceBlockingStub instanceService = factory.create(InstanceServiceBlockingStub.class, InstanceServiceGrpc::newBlockingStub);
        OperationServiceBlockingStub operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        DiskServiceBlockingStub diskService = factory.create(DiskServiceBlockingStub.class, DiskServiceGrpc::newBlockingStub);
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

        List<Disk> disks = diskService.list(buildListDisksRequest()).getDisksList();
        for (Disk disk : disks) {
            Operation deleteOperation = diskService.delete(buildDeleteDiskRequest(disk.getId()));
            System.out.println("Delete disk request sent");

            // Wait for disk deletion
            try {
                OperationUtils.wait(operationService, deleteOperation, Duration.ofMinutes(1));
            } catch (OperationTimeoutException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println(String.format("Deleted disk %s", disk.getId()));
        }
    }

    private static ListDisksRequest buildListDisksRequest() {
        return ListDisksRequest.newBuilder()
                .setFolderId(myFolderId)
                .build();
    }

    private static DeleteDiskRequest buildDeleteDiskRequest(String diskId) {
        return DeleteDiskRequest.newBuilder()
                .setDiskId(diskId)
                .build();
    }

    private static GetImageLatestByFamilyRequest buildGetLatestByFamilyRequest(String familyName) {
        return GetImageLatestByFamilyRequest.newBuilder()
                .setFolderId(imageFolderId)
                .setFamily(familyName)
                .build();
    }

    private static CreateInstanceRequest buildCreateInstanceRequest(String imageId, String name, String zoneId, String subnetId, long memory, long disk, boolean is_public) { // Gb
        Builder netSpec;
        if (is_public) {
            netSpec = NetworkInterfaceSpec.newBuilder()
                        .setSubnetId(subnetId)
                        .setPrimaryV4AddressSpec(PrimaryAddressSpec.newBuilder()
                            .setOneToOneNatSpec(OneToOneNatSpec.newBuilder()
                                .setIpVersion(IpVersion.IPV4)));
        } else {
            netSpec = NetworkInterfaceSpec.newBuilder()
                        .setSubnetId(subnetId)
                        .setPrimaryV4AddressSpec(PrimaryAddressSpec.getDefaultInstance());
        }
        return CreateInstanceRequest.newBuilder()
                .setFolderId(myFolderId)
                .setName(name)
                .setZoneId(zoneId)
                .setPlatformId(Platform.STANDARD_V2.getId())
                // .setResourcesSpec(ResourcesSpec.newBuilder().setCores(2).setCoreFraction(5).setMemory(memory * 1024 * 1024 * 1024))
                .setResourcesSpec(ResourcesSpec.newBuilder().setCores(4).setMemory(memory * 1024 * 1024 * 1024))
                .setBootDiskSpec(AttachedDiskSpec.newBuilder()
                        .setDiskSpec(DiskSpec.newBuilder()
                                .setImageId(imageId)
                                .setSize(disk * 1024 * 1024 * 1024)))
                .addNetworkInterfaceSpecs(netSpec)
                .build();
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

    private static String readFile(String path, Charset encoding)
        throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}
