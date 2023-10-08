package dpl.processing.repository;

import dpl.processing.model.AggregatedData;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AggregatedDataRepository extends CrudRepository<AggregatedData, Long> {

}
