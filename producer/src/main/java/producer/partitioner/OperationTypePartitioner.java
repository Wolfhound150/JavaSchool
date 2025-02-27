package producer.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import sbp.school.kafka.common.enums.OperationType;

import java.util.List;
import java.util.Map;

/*Партишенер по типам операций*/
public class OperationTypePartitioner implements Partitioner {
  @Override
  public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
    List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(s);

    int partCount = partitionInfos.size();
    int operCount = OperationType.values().length;

    if (partCount < operCount) {
      String error = String.format("Number of partitions is less %d", operCount);
      throw new IllegalArgumentException(error);
    }
    return OperationType.valueOf(String.valueOf(o)).getPartNumber();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
