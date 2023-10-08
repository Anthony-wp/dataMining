package dpl.processing.service;

import dpl.processing.model.AggregatedData;
import dpl.processing.repository.AggregatedDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AggregatedDataService {

    private final AggregatedDataRepository repository;

    public AggregatedData saveData(AggregatedData data) {
        return repository.save(data);
    }

}
