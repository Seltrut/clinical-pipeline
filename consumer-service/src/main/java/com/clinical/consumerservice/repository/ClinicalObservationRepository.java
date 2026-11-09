package com.clinical.consumerservice.repository;

import com.clinical.consumerservice.model.ClinicalObservationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ClinicalObservationRepository extends JpaRepository<ClinicalObservationEntity, UUID> {
    boolean existsByEventId(String eventId);
}