/*******************************************************************************
 * Copyright 2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package emlab.gen.role.capacitymarket;

import org.apache.commons.math.stat.regression.SimpleRegression;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import agentspring.role.Role;
import agentspring.role.RoleComponent;
import emlab.gen.domain.agent.EnergyProducer;
import emlab.gen.domain.market.Bid;
import emlab.gen.domain.market.capacity.CapacityDispatchPlan;
import emlab.gen.domain.market.capacity.CapacityMarket;
import emlab.gen.domain.market.electricity.ElectricitySpotMarket;
import emlab.gen.domain.market.electricity.SegmentLoad;
import emlab.gen.domain.technology.PowerPlant;
import emlab.gen.repository.Reps;
import emlab.gen.role.AbstractEnergyProducerRole;

//import org.springframework.data.neo4j.annotation.NodeEntity;

/**
 * @author
 * 
 */

@RoleComponent
public class SubmitCapacityBidToMarketRole extends AbstractEnergyProducerRole<EnergyProducer> implements
        Role<EnergyProducer> {

    Logger logger = Logger.getLogger(SubmitCapacityBidToMarketRole.class);

    @Autowired
    Reps reps;

    @Override
    @Transactional
    public void act(EnergyProducer producer) {
        // logger.warn("***********Submitting Bid Role for Energy Producer ********"
        // + producer.getName());

        for (PowerPlant plant : reps.powerPlantRepository.findOperationalPowerPlantsByOwner(producer, getCurrentTick())) {
            CapacityMarket market = reps.capacityMarketRepository.findCapacityMarketForZone(plant.getLocation()
                    .getZone());
            if (market != null) {
                ElectricitySpotMarket eMarket = reps.marketRepository.findElectricitySpotMarketForZone(plant
                        .getLocation().getZone());

                double mc = 0d;
                double bidPrice = 0d;
                double expectedElectricityRevenues = 0;
                double netRevenues = 0;

                if (getCurrentTick() == 0) {
                    mc = 0;
                    expectedElectricityRevenues = 0d;

                } else {
                    mc = calculateMarginalCostExclCO2MarketCost(plant, getCurrentTick());
                }

                double fixedOnMCost = plant.getActualFixedOperatingCost() / plant.getTechnology().getCapacity();

                // logger.warn("Bid calculation for PowerPlant " +
                // plant.getName());
                // get market for the plant by zone

                // logger.warn("CapacityMarket is  " + market.getName());

                for (SegmentLoad segmentLoad : eMarket.getLoadDurationCurve()) {
                    SimpleRegression sr = new SimpleRegression();
                    double segmentClearingPoint = 0;
                    long time = 0l;
                    for (time = getCurrentTick() - 1; time > getCurrentTick()
                            - plant.getOwner().getNumberOfYearsBacklookingForForecasting()
                            && time > 0; time = time - 1) {

                        double eprice = 0;

                        eprice = reps.segmentClearingPointRepository.findSegmentClearingPointForMarketSegmentAndTime(
                                time, segmentLoad.getSegment(), eMarket, false).getPrice();
                        sr.addData(time, eprice);

                    }
                    if (getCurrentTick() > 2) {
                        double tempSCP = (sr.predict(getCurrentTick()));
                        if (tempSCP < 0) {
                            segmentClearingPoint = 0;
                        } else {
                            segmentClearingPoint = (sr.predict(getCurrentTick()));
                        }

                    } else {
                        if (getCurrentTick() <= 2) {
                            segmentClearingPoint = reps.segmentClearingPointRepository
                                    .findSegmentClearingPointForMarketSegmentAndTime(getCurrentTick() - 1,
                                            segmentLoad.getSegment(), eMarket, false).getPrice();
                        }
                    }

                    double plantLoadFactor = ((plant.getTechnology().getPeakSegmentDependentAvailability()) + (((plant
                            .getTechnology().getBaseSegmentDependentAvailability() - plant.getTechnology()
                            .getPeakSegmentDependentAvailability()) / ((double) (reps.segmentRepository
                            .findBaseSegmentforMarket(eMarket).getSegmentID() - 1))) * (segmentLoad.getSegment()
                            .getSegmentID() - 1)));

                    if (segmentClearingPoint >= mc) {
                        expectedElectricityRevenues = expectedElectricityRevenues
                                + ((segmentClearingPoint - mc) * plant.getActualNominalCapacity() * plantLoadFactor * segmentLoad
                                        .getSegment().getLengthInHours());

                    }

                }

                netRevenues = (expectedElectricityRevenues / plant.getActualNominalCapacity()) - fixedOnMCost;

                if (netRevenues >= 0) {
                    bidPrice = 0d;
                    // } else if (mcCapacity <= fixedOnMCost) {
                } else {
                    bidPrice = netRevenues * (-1);
                }

                double capacity = plant.getActualNominalCapacity()
                        * plant.getTechnology().getPeakSegmentDependentAvailability();

                CapacityDispatchPlan plan = new CapacityDispatchPlan().persist();
                plan.specifyAndPersist(plant, producer, market, getCurrentTick(), bidPrice, capacity, Bid.SUBMITTED);

                // logger.warn("CDP for powerplant " + plan.getPlant().getName()
                // + "rev " + netRevenues);
                // logger.warn("CDP price is " + plan.getPrice());
                // logger.warn("CDP amount is " + plan.getAmount());

            }
        }
    }
}