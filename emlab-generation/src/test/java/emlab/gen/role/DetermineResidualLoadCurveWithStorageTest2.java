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
package emlab.gen.role;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import emlab.gen.domain.agent.DecarbonizationModel;
import emlab.gen.domain.agent.EnergyProducer;
import emlab.gen.domain.agent.Government;
import emlab.gen.domain.agent.NationalGovernment;
import emlab.gen.domain.contract.Loan;
import emlab.gen.domain.gis.Zone;
import emlab.gen.domain.market.CO2Auction;
import emlab.gen.domain.market.ClearingPoint;
import emlab.gen.domain.market.CommodityMarket;
import emlab.gen.domain.market.electricity.ElectricitySpotMarket;
import emlab.gen.domain.market.electricity.Segment;
import emlab.gen.domain.market.electricity.SegmentLoad;
import emlab.gen.domain.technology.Interconnector;
import emlab.gen.domain.technology.IntermittentResourceProfile;
import emlab.gen.domain.technology.PowerGeneratingTechnology;
import emlab.gen.domain.technology.PowerGridNode;
import emlab.gen.domain.technology.PowerPlant;
import emlab.gen.domain.technology.Substance;
import emlab.gen.repository.BidRepository;
import emlab.gen.repository.MarketRepository;
import emlab.gen.repository.PowerPlantDispatchPlanRepository;
import emlab.gen.repository.Reps;
import emlab.gen.repository.SegmentLoadRepository;
import emlab.gen.repository.ZoneRepository;
import emlab.gen.role.market.ClearIterativeCO2AndElectricitySpotMarketTwoCountryRole;
import emlab.gen.role.market.DetermineResidualLoadCurvesForTwoCountriesWithStorageRole;
import emlab.gen.role.market.SubmitOffersToElectricitySpotMarketRole;
import emlab.gen.role.operating.DetermineFuelMixRole;
import emlab.gen.trend.HourlyCSVTimeSeries;
import emlab.gen.trend.LinearTrend;
import emlab.gen.trend.TriangularTrend;

/**
 * @author sophierkerckhoffs
 *
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/emlab-gen-test-context.xml" })
@Transactional
public class DetermineResidualLoadCurveWithStorageTest2 {

    Logger logger = Logger.getLogger(RenewableTargetInvestmentRoleTest.class);

    @Autowired
    Reps reps;

    @Autowired
    SegmentLoadRepository segmentLoadRepository;

    @Autowired
    MarketRepository marketRepository;

    @Autowired
    BidRepository bidRepository;

    @Autowired
    PowerPlantDispatchPlanRepository plantDispatchPlanRepository;

    @Autowired
    ZoneRepository zoneRepository;

    @Autowired
    ClearIterativeCO2AndElectricitySpotMarketTwoCountryRole clearIterativeCO2AndElectricitySpotMarketTwoCountryRole;

    @Autowired
    SubmitOffersToElectricitySpotMarketRole submitOffersToElectricitySpotMarketRole;

    @Autowired
    DetermineFuelMixRole determineFuelMixRole;

    @Autowired
    DetermineResidualLoadCurvesForTwoCountriesWithStorageRole determineResidualLoadCurvesForTwoCountriesWithStorageRole;

    // 6 power plants in two markets, one intermittent power plant in each.
    @Before
    @Transactional
    public void setUp() throws Exception {
        DecarbonizationModel model = new DecarbonizationModel();
        model.setCo2TradingImplemented(false);
        model.setRealRenewableDataImplemented(false);
        model.setIterationSpeedFactor(3);
        model.setIterationSpeedCriterion(0.005);
        model.setCapDeviationCriterion(0.03);
        model.persist();

        Government gov = new Government().persist();
        LinearTrend co2TaxTrend = new LinearTrend().persist();
        co2TaxTrend.setStart(0);
        co2TaxTrend.setIncrement(0);
        gov.setCo2TaxTrend(co2TaxTrend);

        CO2Auction co2Auction = new CO2Auction().persist();

        Zone zone1 = new Zone();

        zone1.setName("Zone 1");

        zone1.persist();

        NationalGovernment natGov1 = new NationalGovernment().persist();

        natGov1.setGovernedZone(zone1);

        LinearTrend minCo2TaxTrend1 = new LinearTrend().persist();
        minCo2TaxTrend1.setStart(0);
        minCo2TaxTrend1.setIncrement(0);
        natGov1.setMinNationalCo2PriceTrend(minCo2TaxTrend1);

        HourlyCSVTimeSeries load1TimeSeries = new HourlyCSVTimeSeries();
        load1TimeSeries.setFilename("/data/ZoneALoad.csv");

        load1TimeSeries.persist();

        PowerGridNode node1 = new PowerGridNode();
        node1.setCapacityMultiplicationFactor(1);
        node1.setZone(zone1);
        node1.setName("Node1");
        node1.setHourlyDemand(load1TimeSeries);
        node1.persist();

        HashSet<PowerGridNode> intNodes = new HashSet<PowerGridNode>();
        intNodes.add(node1);

        Interconnector interconnector = new Interconnector().persist();
        interconnector.setConnections(intNodes);
        interconnector.setCapacity(0);

        Segment S1 = new Segment();
        S1.setLengthInHours(10);
        S1.setSegmentID(1);
        S1.persist();

        Segment S2 = new Segment();
        S2.setLengthInHours(20);
        S2.setSegmentID(2);
        S2.persist();

        Segment S3 = new Segment();
        S3.setLengthInHours(20);
        S3.setSegmentID(3);
        S3.persist();

        Segment S4 = new Segment();
        S4.setLengthInHours(20);
        S4.setSegmentID(4);
        S4.persist();

        SegmentLoad segmentLoadMarket1S2 = new SegmentLoad().persist();
        segmentLoadMarket1S2.setSegment(S2);
        // segmentLoadMarket1S2.setBaseLoad(500.01);

        SegmentLoad segmentLoadMarket1S1 = new SegmentLoad().persist();
        segmentLoadMarket1S1.setSegment(S1);
        // segmentLoadMarket1S1.setBaseLoad(790);

        SegmentLoad segmentLoadMarket1S3 = new SegmentLoad().persist();
        segmentLoadMarket1S3.setSegment(S3);
        // segmentLoadMarket1S1.setBaseLoad(790);

        SegmentLoad segmentLoadMarket1S4 = new SegmentLoad().persist();
        segmentLoadMarket1S4.setSegment(S4);
        // segmentLoadMarket1S1.setBaseLoad(790);

        Set<SegmentLoad> segmentLoads1 = new HashSet<SegmentLoad>();
        segmentLoads1.add(segmentLoadMarket1S1);
        segmentLoads1.add(segmentLoadMarket1S2);
        segmentLoads1.add(segmentLoadMarket1S3);
        segmentLoads1.add(segmentLoadMarket1S4);

        TriangularTrend demandGrowthTrend = new TriangularTrend();
        demandGrowthTrend.setMax(1);
        demandGrowthTrend.setMin(1);
        demandGrowthTrend.setStart(1);
        demandGrowthTrend.setTop(1);

        demandGrowthTrend.persist();

        ElectricitySpotMarket market1 = new ElectricitySpotMarket();
        market1.setName("Market1");
        market1.setZone(zone1);
        market1.setLoadDurationCurve(segmentLoads1);
        market1.setDemandGrowthTrend(demandGrowthTrend);
        market1.setValueOfLostLoad(2000);
        market1.persist();

        Substance coal = new Substance().persist();
        coal.setName("Coal");
        coal.setEnergyDensity(1000);
        Substance gas = new Substance().persist();
        gas.setName("Gas");
        gas.setEnergyDensity(1000);

        CommodityMarket coalMarket = new CommodityMarket().persist();
        CommodityMarket gasMarket = new CommodityMarket().persist();

        coalMarket.setSubstance(coal);
        gasMarket.setSubstance(gas);

        LinearTrend coalPrice = new LinearTrend().persist();
        coalPrice.setStart(3);
        coalPrice.setIncrement(1);

        LinearTrend gasPrice = new LinearTrend().persist();
        gasPrice.setStart(6);
        coalPrice.setIncrement(2);

        HashSet<Substance> fuelMixCoal = new HashSet<Substance>();
        fuelMixCoal.add(coal);

        HashSet<Substance> fuelMixGas = new HashSet<Substance>();
        fuelMixGas.add(gas);

        PowerGeneratingTechnology coalTech = new PowerGeneratingTechnology();
        coalTech.setFuels(fuelMixCoal);
        coalTech.setPeakSegmentDependentAvailability(1);
        coalTech.setBaseSegmentDependentAvailability(1);

        PowerGeneratingTechnology gasTech = new PowerGeneratingTechnology();
        gasTech.setFuels(fuelMixGas);
        gasTech.setPeakSegmentDependentAvailability(1);
        gasTech.setBaseSegmentDependentAvailability(1);

        PowerGeneratingTechnology windTech = new PowerGeneratingTechnology();
        windTech.setName("WindTech");
        windTech.setIntermittent(true);

        PowerGeneratingTechnology storageTech = new PowerGeneratingTechnology();
        storageTech.setName("Storage");
        storageTech.setChargeEfficiency(0.90);
        storageTech.setChargingRate(200);
        storageTech.setDisChargingRate(200);
        storageTech.setMaxStorageCapacity(1000);
        storageTech.setMinStorageCapacity(0);
        storageTech.setStorage(true);
        storageTech.setIntermittent(true);

        coalTech.persist();
        gasTech.persist();
        windTech.persist();
        storageTech.persist();

        IntermittentResourceProfile windIntermittentResourceProfile1 = new IntermittentResourceProfile();
        windIntermittentResourceProfile1.setIntermittentTechnology(windTech);
        windIntermittentResourceProfile1.setIntermittentProductionNode(node1);
        windIntermittentResourceProfile1.setFilename("/data/ResLFA.csv");

        windIntermittentResourceProfile1.persist();

        EnergyProducer market1Prod1 = new EnergyProducer();
        market1Prod1.setName("market1Prod1");
        market1Prod1.setCash(0);
        market1Prod1.setPriceMarkUp(1);
        market1Prod1.setInvestorMarket(market1);

        EnergyProducer market1Prod2 = new EnergyProducer();
        market1Prod2.setCash(0);
        market1Prod2.setPriceMarkUp(1);
        market1Prod2.setName("market1Prod2");
        market1Prod2.setInvestorMarket(market1);

        market1Prod1.persist();
        market1Prod2.persist();

        Loan l1 = new Loan();
        l1.setAmountPerPayment(6000);
        l1.setNumberOfPaymentsDone(10);
        l1.setTotalNumberOfPayments(15);

        Loan l2 = new Loan();
        l2.setAmountPerPayment(5000);
        l2.setNumberOfPaymentsDone(29);
        l2.setTotalNumberOfPayments(19);

        Loan l3 = new Loan();
        l3.setAmountPerPayment(4000);
        l3.setNumberOfPaymentsDone(8);
        l3.setTotalNumberOfPayments(13);

        Loan l4 = new Loan();
        l4.setAmountPerPayment(3000);
        l4.setNumberOfPaymentsDone(7);
        l4.setTotalNumberOfPayments(12);

        Loan l5 = new Loan();
        l5.setAmountPerPayment(2000);
        l5.setNumberOfPaymentsDone(6);
        l5.setTotalNumberOfPayments(11);

        Loan l6 = new Loan();
        l6.setAmountPerPayment(1000);
        l6.setNumberOfPaymentsDone(5);
        l6.setTotalNumberOfPayments(10);

        l1.persist();
        l2.persist();
        l3.persist();
        l4.persist();
        l5.persist();
        l6.persist();

        // At 3 Eur/GJ has a mc of 24 Eur/Mwh
        PowerPlant pp1 = new PowerPlant();
        pp1.setTechnology(coalTech);
        pp1.setOwner(market1Prod1);
        pp1.setActualFixedOperatingCost(99000);
        pp1.setLoan(l1);
        pp1.setActualNominalCapacity(700);
        pp1.setActualEfficiency(0.45);
        pp1.setLocation(node1);
        pp1.setActualPermittime(0);
        pp1.setConstructionStartTime(-2);
        pp1.setActualLeadtime(0);
        pp1.setDismantleTime(10);
        pp1.setExpectedEndOfLife(10);
        pp1.setName("CoalInM1");

        // At 6 Eur/GJ has a mc of 36
        PowerPlant pp3 = new PowerPlant();
        pp3.setTechnology(gasTech);
        pp3.setOwner(market1Prod1);
        pp3.setActualFixedOperatingCost(99000);
        pp3.setLoan(l3);
        pp3.setActualNominalCapacity(650);
        pp3.setActualEfficiency(0.60);
        pp3.setLocation(node1);
        pp3.setActualPermittime(0);
        pp3.setConstructionStartTime(-2);
        pp3.setActualLeadtime(0);
        pp3.setDismantleTime(1000);
        pp3.setExpectedEndOfLife(2);
        pp3.setName("GasInM1");

        pp1.persist();

        pp3.persist();

        // At 6 Eur/GJ has a mc of 36
        PowerPlant ppRes1 = new PowerPlant();
        ppRes1.setTechnology(windTech);
        ppRes1.setOwner(market1Prod1);
        ppRes1.setActualFixedOperatingCost(99000);
        ppRes1.setLoan(l5);
        ppRes1.setActualNominalCapacity(7000);
        ppRes1.setActualEfficiency(1);
        ppRes1.setLocation(node1);
        ppRes1.setActualPermittime(0);
        ppRes1.setConstructionStartTime(-8);
        ppRes1.setActualLeadtime(0);
        ppRes1.setDismantleTime(1000);
        ppRes1.setExpectedEndOfLife(2);
        ppRes1.setName("WindInM1");

        ppRes1.persist();

        PowerPlant ppStorage1 = new PowerPlant();
        ppStorage1.setTechnology(storageTech);
        ppStorage1.setOwner(market1Prod1);
        ppStorage1.setLocation(node1);
        ppStorage1.setName("Storage1");
        ppStorage1.setActualEfficiency(1);
        ppStorage1.setActualPermittime(0);
        ppStorage1.setConstructionStartTime(-8);
        ppStorage1.setActualLeadtime(0);
        ppStorage1.setDismantleTime(1000);
        ppStorage1.setExpectedEndOfLife(2);

        ppStorage1.persist();

        ClearingPoint coalClearingPoint = new ClearingPoint().persist();
        coalClearingPoint.setAbstractMarket(coalMarket);
        coalClearingPoint.setTime(0);
        coalClearingPoint.setPrice(3);
        coalClearingPoint.setVolume(1000);
        coalClearingPoint.setForecast(false);

        ClearingPoint gasClearingPoint = new ClearingPoint().persist();
        gasClearingPoint.setAbstractMarket(gasMarket);
        gasClearingPoint.setTime(0);
        gasClearingPoint.setPrice(6);
        gasClearingPoint.setVolume(1000);
        gasClearingPoint.setForecast(false);
    }

    @Test
    public void determineResidualLoadCurveRoleTest() {

        DecarbonizationModel model = reps.genericRepository.findFirst(DecarbonizationModel.class);

        determineResidualLoadCurvesForTwoCountriesWithStorageRole.act(model);

        for (SegmentLoad segmentLoad : reps.segmentLoadRepository.findAll()) {
            if (segmentLoad.getElectricitySpotMarket().getName() == "Market1") {
                switch (segmentLoad.getSegment().getSegmentID()) {
                case 1:
                    assertEquals("SegmentLoad Market 1, Segment 1", 1156.95, segmentLoad.getBaseLoad(), 0.001);
                    break;
                case 2:
                    assertEquals("SegmentLoad Market 1, Segment 2", 718.95, segmentLoad.getBaseLoad(), 0.001);
                    break;
                }
            } else if (segmentLoad.getElectricitySpotMarket().getName() == "Market2") {
                switch (segmentLoad.getSegment().getSegmentID()) {
                case 1:
                    assertEquals("SegmentLoad Market 2, Segment 1", 2313.9, segmentLoad.getBaseLoad(), 0.001);
                    break;
                case 2:
                    assertEquals("SegmentLoad Market 2, Segment 2", 1437.9, segmentLoad.getBaseLoad(), 0.001);
                    break;
                }
            }
        }
    }

    private long getCurrentTick() {
        return 0;
    }

}
