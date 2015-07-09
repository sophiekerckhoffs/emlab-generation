package emlab.gen.role.market;

import hep.aida.bin.DynamicBin1D;
import ilog.concert.IloException;
import ilog.concert.IloLQNumExpr;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import agentspring.role.AbstractRole;
import agentspring.role.Role;
import agentspring.role.RoleComponent;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.impl.DenseDoubleMatrix1D;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;
import cern.jet.math.Functions;
import emlab.gen.domain.agent.DecarbonizationModel;
import emlab.gen.domain.gis.Zone;
import emlab.gen.domain.market.electricity.IntermittentTechnologyNodeLoadFactor;
import emlab.gen.domain.market.electricity.Segment;
import emlab.gen.domain.market.electricity.SegmentLoad;
import emlab.gen.domain.technology.Interconnector;
import emlab.gen.domain.technology.IntermittentResourceProfile;
import emlab.gen.domain.technology.PowerGeneratingTechnology;
import emlab.gen.domain.technology.PowerGridNode;
import emlab.gen.domain.technology.PowerPlant;
import emlab.gen.repository.Reps;
import emlab.gen.util.Utils;

/**
 * *
 *
 * @author <a href="mailto:E.J.L.Chappin@tudelft.nl">Emile Chappin</a>
 *
 * @author <a href="mailto:J.Richstein@tudelft.nl">Jörn Richstein</a>
 *
 */
@RoleComponent
public class DetermineResidualLoadCurvesForTwoCountriesWithStorageRole extends AbstractRole<DecarbonizationModel>
        implements Role<DecarbonizationModel> {

    @Autowired
    private Reps reps;

    /**
     * Is transactional, since it works a lot on SegmentIntermittentProduction
     * classes.
     */

    @Override
    @Transactional
    public void act(DecarbonizationModel model) {

        long clearingTick = getCurrentTick();

        logger.warn("0. Determining the residual load duration curve");

        // 1. Create big matrix which contains columns for the information later
        // used.
        // Fill the columns with starting information (hour of year, initial
        // maximum interconnector capacity

        // Create Matrix with following columns
        // Hour of year | SegmentId | 2x Load | 2x intermittent Prod. | 2x Res.
        // Load | Res.Load Total | Interc. Cap. | SegmentsAccordingToA |
        // SegmentsAccordingtoB
        // When creating views, and changes are made to the views, the
        // original matrix is changed as well (see Colt package).

        List<Zone> zoneList = Utils.asList(reps.template.findAll(Zone.class));
        List<PowerGeneratingTechnology> intermittentTechnologyList = Utils
                .asList(reps.powerGeneratingTechnologyRepository.findAllIntermittentPowerGeneratingTechnologies());
        // logger.warn("intermittent Technology List" +
        // intermittentTechnologyList.toString());
        List<PowerGeneratingTechnology> storageTechnologyList = Utils.asList(reps.powerGeneratingTechnologyRepository
                .findAllStoragePowerGeneratingTechnologies());
        // logger.warn("Storage Technology List:" +
        // storageTechnologyList.toString());
        List<PowerGeneratingTechnology> technologyList = Utils.asList(reps.powerGeneratingTechnologyRepository
                .findAllStorageAndIntermittentPowerGeneratingTechnologies());
        // logger.warn("Technology List:" + technologyList.toString());

        Map<Zone, List<PowerGridNode>> zoneToNodeList = new HashMap<Zone, List<PowerGridNode>>();
        for (Zone zone : zoneList) {
            List<PowerGridNode> nodeList = Utils.asList(reps.powerGridNodeRepository.findAllPowerGridNodesByZone(zone));
            zoneToNodeList.put(zone, nodeList);
        }

        int columnIterator = 0;

        // Naming of columns, since dynamically created cannot be done as an
        // enum.
        int HOUR = columnIterator;
        columnIterator++;
        int SEGMENT = columnIterator;
        columnIterator++;
        Map<Zone, Integer> LOADINZONE = new HashMap<Zone, Integer>();
        for (Zone zone : zoneList) {
            LOADINZONE.put(zone, columnIterator);
            columnIterator++;
        }

        Map<Zone, Integer> IPROD = new HashMap<Zone, Integer>();
        for (Zone zone : zoneList) {
            IPROD.put(zone, columnIterator);
            columnIterator++;
        }

        Map<Zone, Integer> NETTOCHARGE = new HashMap<Zone, Integer>();
        for (Zone zone : zoneList) {
            NETTOCHARGE.put(zone, columnIterator);
            columnIterator++;
        }

        Map<Zone, Integer> STORAGECAP = new HashMap<Zone, Integer>();
        for (Zone zone : zoneList) {
            STORAGECAP.put(zone, columnIterator);
            columnIterator++;
        }

        Map<Zone, Integer> RLOADINZONE = new HashMap<Zone, Integer>();
        for (Zone zone : zoneList) {
            RLOADINZONE.put(zone, columnIterator);
            columnIterator++;
        }

        Map<Zone, Integer> RLOADINZONERES = new HashMap<Zone, Integer>();
        for (Zone zone : zoneList) {
            RLOADINZONERES.put(zone, columnIterator);
            columnIterator++;
        }

        int RLOADTOTAL = columnIterator;
        columnIterator++;
        int RLOADTOTALRES = columnIterator;
        columnIterator++;
        int INTERCONNECTOR = columnIterator;
        columnIterator++;

        Map<Zone, Integer> SEGMENTFORZONE = new HashMap<Zone, Integer>();
        for (Zone zone : zoneList) {
            SEGMENTFORZONE.put(zone, columnIterator);
            columnIterator++;
        }

        Map<Zone, Map<PowerGridNode, Map<PowerGeneratingTechnology, Integer>>> TECHNOLOGYLOADFACTORSFORZONEANDNODE = new HashMap<Zone, Map<PowerGridNode, Map<PowerGeneratingTechnology, Integer>>>();
        for (Zone zone : zoneList) {
            Map<PowerGridNode, Map<PowerGeneratingTechnology, Integer>> NODETOTECHNOLOGY = new HashMap<PowerGridNode, Map<PowerGeneratingTechnology, Integer>>();
            for (PowerGridNode node : zoneToNodeList.get(zone)) {
                Map<PowerGeneratingTechnology, Integer> technologyToColumn = new HashMap<PowerGeneratingTechnology, Integer>();
                for (PowerGeneratingTechnology technology : technologyList) {
                    technologyToColumn.put(technology, columnIterator);
                    columnIterator++;
                }
                NODETOTECHNOLOGY.put(node, technologyToColumn);
            }
            TECHNOLOGYLOADFACTORSFORZONEANDNODE.put(zone, NODETOTECHNOLOGY);
        }

        double interConnectorCapacity = reps.template.findAll(Interconnector.class).iterator().next()
                .getCapacity(clearingTick);

        // Create globalResidualLoadMatrix and add hours.

        DoubleMatrix2D m = new DenseDoubleMatrix2D(8760, columnIterator);
        m.assign(0d);

        for (int row = 0; row < 8760; row++) {
            m.set(row, HOUR, row);
        }

        // Create vector of 1:
        DoubleMatrix1D oneVector = new DenseDoubleMatrix1D(m.rows());
        oneVector.assign(1);

        // Is set to negative, since later on a max(-interconnector, Rload) is
        // applied.
        m.viewColumn(INTERCONNECTOR).assign(-interConnectorCapacity);

        logger.warn("First 10 values of matrix: \n " + m.viewPart(0, 0, 10, m.columns()).toString());

        // 2. Build national load curves, by adding up grid node load curves in
        // each zone.
        // also fill the residual load columns with the initial load curves.
        // for now simply multiplied with the market wide growth factor
        for (Zone zone : zoneList) {

            for (PowerGridNode node : zoneToNodeList.get(zone)) {
                DoubleMatrix1D hourlyArray = new DenseDoubleMatrix1D(node.getHourlyDemand().getHourlyArray(
                        getCurrentTick()));
                double growthRate = reps.marketRepository.findElectricitySpotMarketForZone(zone).getDemandGrowthTrend()
                        .getValue(clearingTick);
                DoubleMatrix1D growthFactors = hourlyArray.copy();
                growthFactors.assign(growthRate);
                hourlyArray.assign(growthFactors, Functions.mult);
                m.viewColumn(LOADINZONE.get(zone)).assign(hourlyArray, Functions.plus);
                m.viewColumn(RLOADINZONE.get(zone)).assign(hourlyArray, Functions.plus);

            }

        }

        // 3. For each power grid node multiply the time series of each
        // intermittent technology type with
        // the installed capacity of that technology type. Substract
        // intermittent production from the
        // the residual load column (one column per zone). Calculate the total
        // residual load (assuming
        // no interconnector constraints).

        for (Zone zone : zoneList) {

            for (PowerGridNode node : zoneToNodeList.get(zone)) {

                for (PowerGeneratingTechnology technology : intermittentTechnologyList) {

                    double intermittentCapacityOfTechnologyInNode = reps.powerPlantRepository
                            .calculateCapacityOfOperationalIntermittentPowerPlantsByPowerGridNodeAndTechnology(node,
                                    technology, getCurrentTick());

                    logger.warn(technology.getName() + ": " + intermittentCapacityOfTechnologyInNode + " MW in Node "
                            + node.getName() + " and Zone: " + zone.getName());

                    IntermittentResourceProfile intermittentResourceProfile = reps.intermittentResourceProfileRepository
                            .findIntermittentResourceProfileByTechnologyAndNode(technology, node);

                    // Calculates hourly production of intermittent renewable
                    // technology per node
                    DoubleMatrix1D hourlyProductionPerNode = new DenseDoubleMatrix1D(
                            intermittentResourceProfile.getHourlyArray(getCurrentTick()));
                    m.viewColumn(TECHNOLOGYLOADFACTORSFORZONEANDNODE.get(zone).get(node).get(technology)).assign(
                            hourlyProductionPerNode, Functions.plus);
                    hourlyProductionPerNode.assign(Functions.mult(intermittentCapacityOfTechnologyInNode));
                    m.viewColumn(IPROD.get(zone)).assign(hourlyProductionPerNode, Functions.plus);
                    // Add to zonal-technological RES column

                    // Substracts the above from the residual load curve
                    m.viewColumn(RLOADINZONE.get(zone)).assign(hourlyProductionPerNode, Functions.minus);

                }

            }

            m.viewColumn(RLOADTOTAL).assign(m.viewColumn(RLOADINZONE.get(zone)), Functions.plus);
        }
        // 4. Optimize residual load curve for storage and assign new values
        // residual load to matrix

        // Initiate values for optimization

        int HOURS = m.rows(); // amount of hours in simulation

        List<PowerPlant> storagePowerPlantList = new ArrayList<PowerPlant>();

        for (Zone zone : zoneList) {
            for (PowerGridNode node : zoneToNodeList.get(zone)) {

                storagePowerPlantList.add(reps.powerPlantRepository.findOperationalStoragePowerPlantsByPowerGridNode(
                        node, getCurrentTick()));

            }
        }

        double[] initialStorage = new double[storagePowerPlantList.size()];
        if (!storagePowerPlantList.contains(null)) {

            if (getCurrentTick() == 0) {
                for (PowerPlant pp : storagePowerPlantList) {
                    initialStorage[storagePowerPlantList.indexOf(pp)] = 0;
                }
            } else {
                for (PowerPlant pp : storagePowerPlantList) {
                    initialStorage[storagePowerPlantList.indexOf(pp)] = pp.getActualStorageContentEndOfYear();
                }
            }

            // Start optimization model

            try {
                // define new model
                IloCplex cplex = new IloCplex();

                // define variables

                IloNumVar[][] P = new IloNumVar[zoneList.size()][HOURS]; // Load
                // in
                // zone
                for (Zone zone : zoneList) {
                    P[zoneList.indexOf(zone)] = cplex
                            .numVarArray(HOURS, zoneList.indexOf(zone), zoneList.indexOf(zone));
                    for (int hour = 0; hour < HOURS; hour++) {
                        P[zoneList.indexOf(zone)][hour] = cplex.numVar(m.get(hour, RLOADINZONE.get(zone)),
                                m.get(hour, RLOADINZONE.get(zone)));
                    }
                }

                IloNumVar[][] E = new IloNumVar[zoneList.size()][HOURS]; // Energy
                // storage
                // content in
                // Zone j at time
                // i
                for (Zone zone : zoneList) {
                    E[zoneList.indexOf(zone)] = cplex
                            .numVarArray(HOURS, zoneList.indexOf(zone), zoneList.indexOf(zone));
                    for (int hour = 0; hour < HOURS; hour++) {
                        E[zoneList.indexOf(zone)][hour] = cplex.numVar(storagePowerPlantList
                                .get(zoneList.indexOf(zone)).getTechnology().getMinStorageCapacity(),
                                storagePowerPlantList.get(zoneList.indexOf(zone)).getTechnology()
                                        .getMaxStorageCapacity());
                    }
                }

                IloNumVar[][] mIn = new IloNumVar[zoneList.size()][HOURS]; // Power
                // outflow of
                // market j at
                // time i
                for (Zone zone : zoneList) {
                    mIn[zoneList.indexOf(zone)] = cplex.numVarArray(HOURS, zoneList.indexOf(zone),
                            zoneList.indexOf(zone));
                    for (int hour = 0; hour < HOURS; hour++) {
                        mIn[zoneList.indexOf(zone)][hour] = cplex.numVar(0, Double.MAX_VALUE);
                    }
                }

                IloNumVar[][] mOut = new IloNumVar[zoneList.size()][HOURS]; // Power
                // inflow of
                // market j at
                // time i
                for (Zone zone : zoneList) {
                    mOut[zoneList.indexOf(zone)] = cplex.numVarArray(HOURS, zoneList.indexOf(zone),
                            zoneList.indexOf(zone));
                    for (int hour = 0; hour < HOURS; hour++) {
                        mOut[zoneList.indexOf(zone)][hour] = cplex.numVar(0, Double.MAX_VALUE);
                    }
                }

                IloNumVar[][] sIn = new IloNumVar[zoneList.size()][HOURS]; // Storage
                // inflow in
                // storage j at
                // time i
                for (Zone zone : zoneList) {
                    sIn[zoneList.indexOf(zone)] = cplex.numVarArray(HOURS, zoneList.indexOf(zone),
                            zoneList.indexOf(zone));
                    for (int hour = 0; hour < HOURS; hour++) {
                        sIn[zoneList.indexOf(zone)][hour] = cplex.numVar(0,
                                storagePowerPlantList.get(zoneList.indexOf(zone)).getTechnology().getChargingRate());
                    }
                }

                IloNumVar[][] sOut = new IloNumVar[zoneList.size()][HOURS]; // Storage
                // outflow of
                // storage j
                // at time i
                for (Zone zone : zoneList) {
                    sOut[zoneList.indexOf(zone)] = cplex.numVarArray(HOURS, zoneList.indexOf(zone),
                            zoneList.indexOf(zone));
                    for (int hour = 0; hour < HOURS; hour++) {
                        sOut[zoneList.indexOf(zone)][hour] = cplex.numVar(0,
                                storagePowerPlantList.get(zoneList.indexOf(zone)).getTechnology().getDisChargingRate());
                    }
                }

                IloNumVar[] I = new IloNumVar[HOURS]; // Power flow from zone A
                // to B
                for (int hour = 0; hour < HOURS; hour++) {
                    I[hour] = cplex.numVar(-interConnectorCapacity, interConnectorCapacity);
                }

                // define expressions

                IloLinearNumExpr[][] storageContent = new IloLinearNumExpr[zoneList.size()][HOURS];
                for (int zones = 0; zones < zoneList.size(); zones++) {
                    for (int hour = 1; hour < HOURS; hour++) {
                        storageContent[zones][hour] = cplex.linearNumExpr();
                        storageContent[zones][hour].addTerm(1.0, E[zones][hour - 1]);
                        storageContent[zones][hour].addTerm(storagePowerPlantList.get(zones).getTechnology()
                                .getChargeEfficiency(), sIn[zones][hour - 1]);
                        storageContent[zones][hour].addTerm(
                                (storagePowerPlantList.get(zones).getTechnology().getDisChargeEfficiency()),
                                sOut[zones][hour - 1]);
                    }
                }

                IloLinearNumExpr[][] marketOutflow = new IloLinearNumExpr[zoneList.size()][HOURS];
                for (int zones = 0; zones < zoneList.size(); zones++) {
                    for (int hour = 0; hour < HOURS; hour++) {
                        if (zones == 0) {
                            marketOutflow[zones][hour] = cplex.linearNumExpr();
                            marketOutflow[zones][hour].addTerm(1, sIn[zones][hour]);
                            marketOutflow[zones][hour].addTerm(1, I[hour]);
                        }
                        if (zones == 1) {
                            marketOutflow[zones][hour] = cplex.linearNumExpr();
                            marketOutflow[zones][hour].addTerm(1, sIn[zones][hour]);
                            marketOutflow[zones][hour].addTerm(-1, I[hour]);
                        }
                    }
                }

                IloLinearNumExpr[][] marketInflow = new IloLinearNumExpr[zoneList.size()][HOURS];
                for (int zones = 0; zones < zoneList.size(); zones++) {
                    for (int hour = 0; hour < HOURS; hour++) {
                        if (zones == 0) {
                            marketInflow[zones][hour] = cplex.linearNumExpr();
                            marketInflow[zones][hour].addTerm(1, sOut[zones][hour]);
                            marketInflow[zones][hour].addTerm(-1, I[hour]);
                        }
                        if (zones == 1) {
                            marketInflow[zones][hour] = cplex.linearNumExpr();
                            marketInflow[zones][hour].addTerm(1, sOut[zones][hour]);
                            marketInflow[zones][hour].addTerm(1, I[hour]);
                        }
                    }
                }

                IloLQNumExpr objective = cplex.lqNumExpr();
                for (int zones = 0; zones < zoneList.size(); zones++) {
                    for (int hour = 0; hour < HOURS; hour++) {
                        objective.addTerm(1, P[zones][hour], P[zones][hour]);
                        objective.addTerm(1, mIn[zones][hour], mIn[zones][hour]);
                        objective.addTerm(1, mOut[zones][hour], mOut[zones][hour]);
                        objective.addTerm(2, P[zones][hour], mIn[zones][hour]);
                        objective.addTerm(-2, P[zones][hour], mOut[zones][hour]);
                        objective.addTerm(-2, mIn[zones][hour], mOut[zones][hour]);
                    }
                }

                // define objective function
                cplex.addMinimize(objective);

                // define constraints

                for (int zones = 0; zones < zoneList.size(); zones++) {
                    cplex.addEq(E[zones][0], initialStorage[zones]);
                }

                for (int zones = 0; zones < zoneList.size(); zones++) {
                    cplex.addLe(sOut[zones][0], E[zones][0]);
                }

                for (int zones = 0; zones < zoneList.size(); zones++) {
                    for (int hour = 1; hour < HOURS; hour++) {
                        cplex.addEq(storageContent[zones][hour], E[zones][hour]);
                    }
                }

                for (int zones = 0; zones < zoneList.size(); zones++) {
                    for (int hour = 1; hour < HOURS; hour++) {
                        cplex.addEq(storageContent[zones][hour], E[zones][hour]);
                    }
                }

                for (int zones = 0; zones < zoneList.size(); zones++) {
                    for (int hour = 0; hour < HOURS; hour++) {
                        cplex.or(cplex.addEq(mOut[zones][hour], marketOutflow[zones][hour]),
                                cplex.addEq(mIn[zones][hour], marketInflow[zones][hour]));
                    }
                }

                // solve model
                if (cplex.solve()) {

                    double[] nettoChargeZoneA = new double[8760];
                    double[] nettoChargeZoneB = new double[8760];
                    double[] usedStorageCapacityA = new double[8760];
                    double[] usedStorageCapacityB = new double[8760];

                    for (int zones = 0; zones < zoneList.size(); zones++) {
                        for (int hour = 0; hour < HOURS; hour++) {
                            if (zones == 0) {
                                nettoChargeZoneA[hour] = cplex.getValue(sIn[zones][hour])
                                        - cplex.getValue(sOut[zones][hour]);
                                usedStorageCapacityA[hour] = cplex.getValue(E[zones][hour]);
                            }
                            if (zones == 1) {
                                nettoChargeZoneB[hour] = cplex.getValue(sIn[zones][hour])
                                        - cplex.getValue(sOut[zones][hour]);
                                usedStorageCapacityB[hour] = cplex.getValue(E[zones][hour]);
                            }
                        }
                    }

                    DoubleMatrix1D nettoChargeZoneAVector = new DenseDoubleMatrix1D(nettoChargeZoneA);
                    DoubleMatrix1D nettoChargeZoneBVector = new DenseDoubleMatrix1D(nettoChargeZoneB);
                    DoubleMatrix1D usedStorageCapacityAVector = new DenseDoubleMatrix1D(usedStorageCapacityA);
                    DoubleMatrix1D usedStorageCapacityBVector = new DenseDoubleMatrix1D(usedStorageCapacityB);

                    for (int zones = 0; zones < zoneList.size(); zones++) {
                        if (zones == 0) {
                            m.viewColumn(NETTOCHARGE.get(zoneList.get(zones))).assign(nettoChargeZoneAVector,
                                    Functions.plus);
                            m.viewColumn(STORAGECAP.get(zoneList.get(zones))).assign(usedStorageCapacityAVector,
                                    Functions.plus);
                        }
                        if (zones == 1) {
                            m.viewColumn(NETTOCHARGE.get(zoneList.get(zones))).assign(nettoChargeZoneBVector,
                                    Functions.plus);
                            m.viewColumn(STORAGECAP.get(zoneList.get(zones))).assign(usedStorageCapacityBVector,
                                    Functions.plus);
                        }
                    }

                    double[] interConnector = new double[8760];

                    for (int hour = 0; hour < HOURS; hour++) {
                        interConnector[hour] = cplex.getValue(I[hour]);
                    }

                    DoubleMatrix1D interConnectorVector = new DenseDoubleMatrix1D(interConnector);
                    m.viewColumn(INTERCONNECTOR).assign(interConnectorVector, Functions.plus);

                    for (Zone zone : zoneList) {

                        for (PowerGridNode node : zoneToNodeList.get(zone)) {

                            for (PowerGeneratingTechnology technology : storageTechnologyList) {

                                DoubleMatrix1D maxStorageCapacity = new DenseDoubleMatrix1D(8760);
                                maxStorageCapacity.assign(technology.getMaxStorageCapacity());

                                m.viewColumn(TECHNOLOGYLOADFACTORSFORZONEANDNODE.get(zone).get(node).get(technology))
                                        .assign(m.viewColumn(NETTOCHARGE.get(zone)), Functions.plus);
                                m.viewColumn(TECHNOLOGYLOADFACTORSFORZONEANDNODE.get(zone).get(node).get(technology))
                                        .assign(maxStorageCapacity, Functions.div);

                            }
                        }
                    }

                    for (PowerPlant pp : storagePowerPlantList) {
                        pp.setActualStorageContentEndOfYear(cplex.getValue(E[storagePowerPlantList.indexOf(pp)][8759]));

                    }

                } else {
                    logger.warn("Model did not solve");
                }

                // close cplex
                cplex.end();

            } catch (IloException exc) {
                exc.printStackTrace();
            }

            // Assign new values to matrix

            for (Zone zone : zoneList) {
                for (PowerGridNode node : zoneToNodeList.get(zone)) {

                    m.viewColumn(RLOADINZONERES.get(zone)).assign(m.viewColumn(RLOADINZONE.get(zone)));

                    m.viewColumn(RLOADINZONE.get(zone)).assign(m.viewColumn(NETTOCHARGE.get(zone)), Functions.minus);

                }
                m.viewColumn(RLOADTOTALRES).assign(m.viewColumn(RLOADTOTAL), Functions.plus);
                m.viewColumn(RLOADTOTAL).assign(m.viewColumn(RLOADINZONE.get(zone)));
            }
        }

        logger.warn("First 10 values of matrix: \n" + m.viewPart(0, 0, 10, m.columns()).toString());
        logger.warn("Last 10 values of matrix: \n" + m.viewPart(8750, 0, 10, m.columns()).toString());

        // 5. Reduce the load factors by obvious
        // spill, that is RES production & storage greater than demand +
        // interconnector
        // capacity.

        // Assign minimum of -interConnectorCapacity to national
        // residual load

        for (Zone zone : zoneList) {
            m.viewColumn(RLOADINZONE.get(zone)).assign(m.viewColumn(INTERCONNECTOR), Functions.max);

            // Assign a maximum production value of Load +
            // interconnector capacity of INTPROD
            DoubleMatrix1D loadPlusInterconnector = m.viewColumn(LOADINZONE.get(zone)).copy();
            // Need to substract interconnector capacity, since defined
            // negatively.
            loadPlusInterconnector.assign(m.viewColumn(INTERCONNECTOR), Functions.minus);
            DoubleMatrix1D spillVector = loadPlusInterconnector.copy();
            spillVector.assign(m.viewColumn(IPROD.get(zone)), Functions.div);
            spillVector.assign(oneVector, Functions.min);
            m.viewColumn(IPROD.get(zone)).assign(loadPlusInterconnector, Functions.minus);

            for (PowerGridNode node : zoneToNodeList.get(zone)) {

                for (PowerGeneratingTechnology technology : technologyList) {
                    m.viewColumn(TECHNOLOGYLOADFACTORSFORZONEANDNODE.get(zone).get(node).get(technology)).assign(
                            spillVector, Functions.mult);
                }
            }

        }

        // 6. Do a pre-market clearing of RES production: For each time step
        // check if there's negative residual loads
        // in each country. Export from one country to another country if
        // interconnector constraints and residual
        // and residual load in the other country allow for that. Reduce
        // intermittent production if over supply.
        // In the end calculate the total residual load curve over all
        // countries.
        // logger.warn("First 10 values of matrix: \n " + m.viewPart(0, 0, 10,
        // m.columns()).toString());

        Map<Zone, DoubleMatrix1D> spillFactorMap = new HashMap<Zone, DoubleMatrix1D>();
        // Get old values of IPROD to calculate spill factors later on.
        for (Zone zone : zoneList) {
            spillFactorMap.put(zone, m.viewColumn(IPROD.get(zone)).copy());
        }

        if (zoneList.size() > 1) {

            Zone zoneA = zoneList.get(0);
            Zone zoneB = zoneList.get(1);

            Zone zoneSmallerResidual;
            Zone zoneBiggerResidual;

            int numberOfHoursWereBothCountriesHaveNegativeResidualLoad = 0;
            int numberOfHoursWhereOneCountryExportsREStoTheOther = 0;
            int printAmount = 0;

            for (int row = 0; row < m.rows(); row++) {

                if (m.get(row, RLOADINZONE.get(zoneA)) < m.get(row, RLOADINZONE.get(zoneB))) {
                    zoneSmallerResidual = zoneA;
                    zoneBiggerResidual = zoneB;
                } else {
                    zoneSmallerResidual = zoneB;
                    zoneBiggerResidual = zoneA;
                }

                double smallerResidual = m.viewColumn(RLOADINZONE.get(zoneSmallerResidual)).get(row);
                double biggerResidual = m.viewColumn(RLOADINZONE.get(zoneBiggerResidual)).get(row);
                // In case both countries have negative residual load (more
                // IPROD
                // than load), set RLOAD to zero, and reduce IPROD to LOAD in
                // countries. Calculate the spill factor of the two zones, and
                // multiply it
                // to the load factors of the technologies in the respective
                // nodes.
                if ((smallerResidual <= 0) && biggerResidual <= 0) {
                    numberOfHoursWereBothCountriesHaveNegativeResidualLoad++;
                    m.set(row, RLOADINZONE.get(zoneSmallerResidual), 0);
                    m.set(row, RLOADINZONE.get(zoneBiggerResidual), 0);
                    m.viewColumn(IPROD.get(zoneSmallerResidual)).set(row,
                            m.get(row, LOADINZONE.get(zoneSmallerResidual)));
                    m.viewColumn(IPROD.get(zoneBiggerResidual))
                            .set(row, m.get(row, LOADINZONE.get(zoneBiggerResidual)));
                } else if ((smallerResidual < 0) && (biggerResidual > 0)) {
                    numberOfHoursWhereOneCountryExportsREStoTheOther++;
                    // In case the country with the smaller residual can export
                    // and
                    // bigger residual can import, check what is the limiting
                    // factor.
                    double diffSandB = smallerResidual + biggerResidual;
                    // Country BiggerResidual can import more than Country
                    // SmallerResidual can export
                    if (diffSandB > 0) {
                        // Interconnector capacity is not limiting
                        if (Math.abs(smallerResidual) < Math.abs(m.get(row, INTERCONNECTOR))) {
                            // Substract export IPROD from interconnector
                            // capacity,
                            // reduce RLOAD in CountyrSmallerResidual to 0, in
                            // CountryBiggerResidual to
                            // biggerResidual+smallerResidual
                            m.set(row, INTERCONNECTOR, (m.get(row, INTERCONNECTOR) - smallerResidual));
                            m.viewColumn(RLOADINZONE.get(zoneSmallerResidual)).set(row, 0);
                            m.viewColumn(RLOADINZONE.get(zoneBiggerResidual))
                                    .set(row, biggerResidual + smallerResidual);
                        } else {
                            m.set(row, INTERCONNECTOR, 0);
                            m.viewColumn(RLOADINZONE.get(zoneSmallerResidual)).set(row, 0);
                            m.viewColumn(RLOADINZONE.get(zoneBiggerResidual)).set(row,
                                    biggerResidual + m.get(row, INTERCONNECTOR));
                        }
                    } else {
                        // Country BiggerResidual can import less than Country
                        // SmallerResidual could export
                        // Interconnector capacity is not limiting
                        if (Math.abs(smallerResidual) < Math.abs(m.viewColumn(INTERCONNECTOR).get(row))) {
                            m.set(row, INTERCONNECTOR, m.get(row, INTERCONNECTOR) + biggerResidual);
                            m.set(row, RLOADINZONE.get(zoneBiggerResidual), 0);
                            m.set(row, RLOADINZONE.get(zoneSmallerResidual), 0);
                            m.set(row, IPROD.get(zoneSmallerResidual),
                                    (m.get(row, IPROD.get(zoneSmallerResidual)) - biggerResidual));
                        } else {
                            // Interconnector capacity is limiting
                            m.set(row, INTERCONNECTOR, 0);
                            m.set(row, RLOADINZONE.get(zoneBiggerResidual),
                                    biggerResidual + m.viewColumn(INTERCONNECTOR).get(row));
                            m.set(row, RLOADINZONE.get(zoneSmallerResidual), 0);
                            m.set(row, IPROD.get(zoneSmallerResidual),
                                    (m.get(row, IPROD.get(zoneSmallerResidual)) - m.get(row, INTERCONNECTOR)));
                        }
                    }
                }

                m.viewColumn(RLOADTOTAL).set(
                        row,
                        m.get(row, RLOADINZONE.get(zoneSmallerResidual))
                                + m.get(row, RLOADINZONE.get(zoneBiggerResidual)));
            }

            // First divide it by new value. Spilled values are than greater
            // than 1, the other equal to 1.
            for (Zone zone : zoneList) {
                DoubleMatrix1D minValuesVector = spillFactorMap.get(zone).like();
                minValuesVector.assign(Double.MIN_NORMAL);
                spillFactorMap.get(zone).assign(minValuesVector, Functions.plus);
                m.viewColumn(IPROD.get(zone)).assign(minValuesVector, Functions.plus);
                spillFactorMap.get(zone).assign(m.viewColumn(IPROD.get(zone)), Functions.div);
                m.viewColumn(IPROD.get(zone)).assign(minValuesVector, Functions.minus);
            }

            oneVector.assign(spillFactorMap.get(zoneList.get(0)), Functions.minus);

            DoubleMatrix1D differenceVector = m.viewColumn(12).copy();
            differenceVector.assign(m.viewColumn(13), Functions.minus);
            double result = differenceVector.aggregate(Functions.plus, Functions.identity);
            // logger.warn("Result: " + result);
            // Divide all the technology load factors in the zone by the spill
            // factors above
            for (Zone zone : zoneList) {
                for (PowerGridNode node : zoneToNodeList.get(zone)) {
                    for (PowerGeneratingTechnology technology : technologyList) {
                        m.viewColumn(TECHNOLOGYLOADFACTORSFORZONEANDNODE.get(zone).get(node).get(technology)).assign(
                                spillFactorMap.get(zone), Functions.div);
                    }
                }
            }
        }

        // Make the interconnector capacity postive
        m.viewColumn(INTERCONNECTOR).assign(Functions.mult(-1));

        // logger.warn("Number of hours where both countries of negative residual load: "
        // + numberOfHoursWereBothCountriesHaveNegativeResidualLoad);
        // logger.warn("Number of hours where one country exports to the other: "
        // + numberOfHoursWhereOneCountryExportsREStoTheOther);

        // 7. Order the hours in the global residual load curve. Peak load
        // first, base load last.

        // Sorts matrix by the load curve in descending order

        m = m.viewSorted(RLOADTOTAL).viewRowFlip();

        // 6. Find values, so that each segments has approximately equal
        // capacity
        // needs.

        double min = m.viewColumn(RLOADTOTAL).aggregate(Functions.min, Functions.identity);
        double max = m.viewColumn(RLOADTOTAL).aggregate(Functions.max, Functions.identity);
        double hoursWithoutZeroRLoad = 0;
        for (int row = 0; row < m.rows(); row++) {
            if (m.get(row, RLOADTOTAL) > 0) {
                hoursWithoutZeroRLoad++;
            } else {
                break;
            }

        }

        int noSegments = (int) reps.segmentRepository.count();

        double[] upperBoundSplit = new double[noSegments];

        if (hoursWithoutZeroRLoad > 8750) {
            for (int i = 0; i < noSegments; i++) {
                upperBoundSplit[i] = max - (((double) (i)) / noSegments * (max - min));
            }
        } else {
            for (int i = 0; i < (noSegments - 1); i++) {
                upperBoundSplit[i] = max - (((double) (i)) / (noSegments - 1) * (max - min));
            }
            upperBoundSplit[noSegments - 1] = 0;
        }

        // 8. Create DynamicBins as representation for segments and for later
        // calculation of means, no etc. Per bin one sort of information (e.g.
        // residual
        // load, interconnector capacity) and the corresponding hour of the yea
        // can be stored.
        // Thus connection to the matrix remains.

        DynamicBin1D[] segmentRloadBins = new DynamicBin1D[noSegments];
        for (int i = 0; i < noSegments; i++) {
            segmentRloadBins[i] = new DynamicBin1D();
        }

        DynamicBin1D[] segmentInterConnectorBins = new DynamicBin1D[noSegments];
        for (int i = 0; i < noSegments; i++) {
            segmentInterConnectorBins[i] = new DynamicBin1D();
        }

        Map<Zone, DynamicBin1D[]> segmentRloadBinsByZone = new HashMap<Zone, DynamicBin1D[]>();
        Map<Zone, DynamicBin1D[]> segmentLoadBinsByZone = new HashMap<Zone, DynamicBin1D[]>();

        for (Zone zone : zoneList) {
            DynamicBin1D[] segmentRloadBinInZone = new DynamicBin1D[noSegments];
            DynamicBin1D[] segmentLoadBinInZone = new DynamicBin1D[noSegments];
            for (int i = 0; i < noSegments; i++) {
                segmentRloadBinInZone[i] = new DynamicBin1D();
                segmentLoadBinInZone[i] = new DynamicBin1D();
            }
            segmentRloadBinsByZone.put(zone, segmentRloadBinInZone);
            segmentLoadBinsByZone.put(zone, segmentLoadBinInZone);
        }

        Map<Zone, Map<PowerGridNode, Map<PowerGeneratingTechnology, DynamicBin1D[]>>> loadFactorBinMap = new HashMap<Zone, Map<PowerGridNode, Map<PowerGeneratingTechnology, DynamicBin1D[]>>>();
        for (Zone zone : zoneList) {
            Map<PowerGridNode, Map<PowerGeneratingTechnology, DynamicBin1D[]>> NODETOTECHNOLOGY = new HashMap<PowerGridNode, Map<PowerGeneratingTechnology, DynamicBin1D[]>>();
            for (PowerGridNode node : zoneToNodeList.get(zone)) {
                Map<PowerGeneratingTechnology, DynamicBin1D[]> technologyToBins = new HashMap<PowerGeneratingTechnology, DynamicBin1D[]>();
                for (PowerGeneratingTechnology technology : technologyList) {
                    DynamicBin1D[] technologyLoadFactorInNode = new DynamicBin1D[noSegments];
                    for (int i = 0; i < noSegments; i++) {
                        technologyLoadFactorInNode[i] = new DynamicBin1D();
                    }
                    technologyToBins.put(technology, technologyLoadFactorInNode);
                }
                NODETOTECHNOLOGY.put(node, technologyToBins);
            }
            loadFactorBinMap.put(zone, NODETOTECHNOLOGY);
        }

        // logger.warn("Max: " + max + "\n" + "Min: " + min);
        // for (double value : upperBoundSplit) {
        // logger.warn("Split-Value:" + value);
        // }

        // Assign hours and load to bins and segments
        int currentSegmentID = 1;
        int hoursAssignedToCurrentSegment = 0;
        for (int row = 0; row < m.rows() && currentSegmentID <= noSegments; row++) {
            // IMPORTANT: since [] is zero-based index, it checks one index
            // ahead of current segment.
            while (currentSegmentID < noSegments && hoursAssignedToCurrentSegment > 0
                    && m.get(row, RLOADTOTAL) <= upperBoundSplit[currentSegmentID]) {
                currentSegmentID++;
                hoursAssignedToCurrentSegment = 0;
            }
            m.set(row, SEGMENT, currentSegmentID);
            segmentRloadBins[currentSegmentID - 1].add(m.get(row, RLOADTOTAL));
            for (Zone zone : zoneList) {
                segmentRloadBinsByZone.get(zone)[currentSegmentID - 1].add(m.get(row, RLOADINZONE.get(zone)));
                segmentLoadBinsByZone.get(zone)[currentSegmentID - 1].add(m.get(row, LOADINZONE.get(zone)));
            }
            segmentInterConnectorBins[currentSegmentID - 1].add(m.get(row, INTERCONNECTOR));
            hoursAssignedToCurrentSegment++;
        }

        for (Zone zone : zoneList) {
            for (PowerGridNode node : zoneToNodeList.get(zone)) {
                for (PowerGeneratingTechnology technology : reps.powerGeneratingTechnologyRepository
                        .findAllStorageAndIntermittentPowerGeneratingTechnologies()) {
                    DynamicBin1D[] currentBinArray = loadFactorBinMap.get(zone).get(node).get(technology);
                    int columnNumber = TECHNOLOGYLOADFACTORSFORZONEANDNODE.get(zone).get(node).get(technology);
                    currentSegmentID = 1;
                    hoursAssignedToCurrentSegment = 0;
                    for (int row = 0; row < m.rows() && currentSegmentID <= noSegments; row++) {
                        // IMPORTANT: since [] is zero-based index, it checks
                        // one index
                        // ahead of current segment.
                        while (currentSegmentID < noSegments && hoursAssignedToCurrentSegment > 0
                                && m.get(row, RLOADTOTAL) <= upperBoundSplit[currentSegmentID]) {
                            currentSegmentID++;
                            hoursAssignedToCurrentSegment = 0;
                        }
                        currentBinArray[currentSegmentID - 1].add(m.get(row, columnNumber));
                        hoursAssignedToCurrentSegment++;
                    }
                    loadFactorBinMap.get(zone).get(node).put(technology, currentBinArray);
                }
            }
        }

        // Assign hours to segments according to residual load in this country.
        // Only for error estimation purposes

        for (Zone zone : zoneList) {
            currentSegmentID = 1;
            double minInZone = m.viewColumn(RLOADINZONE.get(zone)).aggregate(Functions.min, Functions.identity);
            double maxInZone = m.viewColumn(RLOADINZONE.get(zone)).aggregate(Functions.max, Functions.identity);

            double[] upperBoundSplitInZone = new double[noSegments];

            for (int i = 0; i < noSegments; i++) {
                upperBoundSplitInZone[i] = maxInZone - (((double) (i)) / noSegments * (maxInZone - minInZone));
            }

            m = m.viewSorted(RLOADINZONE.get(zone)).viewRowFlip();
            int hoursInDifferentSegment = 0;
            double averageSegmentDeviation = 0;
            hoursAssignedToCurrentSegment = 0;
            for (int row = 0; row < m.rows() && currentSegmentID <= noSegments; row++) {
                while (currentSegmentID < noSegments && hoursAssignedToCurrentSegment > 0
                        && m.get(row, RLOADINZONE.get(zone)) <= upperBoundSplitInZone[currentSegmentID]) {
                    currentSegmentID++;
                    hoursAssignedToCurrentSegment = 0;
                }

                m.set(row, SEGMENTFORZONE.get(zone), currentSegmentID);
                if (currentSegmentID != m.get(row, SEGMENT)) {
                    hoursInDifferentSegment++;
                    averageSegmentDeviation += Math.abs(currentSegmentID - m.get(row, SEGMENT));
                }
                hoursAssignedToCurrentSegment++;
            }

            if (hoursInDifferentSegment != 0) {
                averageSegmentDeviation = averageSegmentDeviation / hoursInDifferentSegment;
                averageSegmentDeviation = averageSegmentDeviation * 1000;
                averageSegmentDeviation = Math.round(averageSegmentDeviation);
                averageSegmentDeviation = averageSegmentDeviation / 1000;
                logger.warn("For " + zone + ", " + hoursInDifferentSegment
                        + " hours would have been in different segments, and on average " + averageSegmentDeviation
                        + " Segments away from the segment they were in.");
            } else {
                logger.warn("For " + zone + ", all hours were in the same segment, as for combined sorting!");
            }

        }

        // m = m.viewSorted(RLOADTOTAL).viewRowFlip();
        //
        // logger.debug("First 30 values of matrix: \n " + m.viewPart(0, 0, 30,
        // m.columns()).toString());

        // Printing of segments
        int it = 1;
        for (DynamicBin1D bin : segmentRloadBins) {
            // logger.warn("Segment " + it + "\n      Size: " + bin.size() +
            // "\n      Mean RLOAD~: " + Math.round(bin.mean())
            // + "\n      Max RLOAD~: " + Math.round(bin.max()) +
            // "\n      Min RLOAD~: " + Math.round(bin.min())
            // + "\n      Std RLOAD~: " + Math.round(bin.standardDeviation()));
            it++;
        }

        it = 1;
        for (DynamicBin1D bin : segmentInterConnectorBins) {
            // logger.warn("Segment " + it + "\n      Size: " + bin.size() +
            // "\n      Mean IntCapacity~: "
            // + Math.round(bin.mean()) + "\n      Max IntCapacity~: " +
            // Math.round(bin.max())
            // + "\n      Min IntCapacity~: " + Math.round(bin.min()) +
            // "\n      STD IntCapacity~: "
            // + Math.round(bin.standardDeviation()));
            it++;
        }

        for (Zone zone : zoneList) {
            // logger.warn("Bins for " + zone);
            it = 1;
            String meanRLoad = new String("Residual load in " + zone.getName() + ":");
            String meanLoad = new String("Load in " + zone.getName() + ":");
            String segmentLength = new String("Segment length " + zone.getName() + ":");
            for (DynamicBin1D bin : segmentRloadBinsByZone.get(zone)) {
                // logger.warn("Segment " + it + "\n      Size: " + bin.size() +
                // "\n      Mean RLOAD~: " + Math.round(bin.mean())
                // + "\n      Max RLOAD~: " + Math.round(bin.max()) +
                // "\n      Min RLOAD~: " + Math.round(bin.min())
                // + "\n      Std RLOAD~: " +
                // Math.round(bin.standardDeviation()));
                it++;
                double mean = bin.mean() * 1000;
                mean = Math.round(mean);
                mean = mean / 1000.0;
                meanRLoad = meanRLoad.concat("," + mean);
                segmentLength = segmentLength.concat("," + bin.size());
            }
            it = 1;
            for (DynamicBin1D bin : segmentLoadBinsByZone.get(zone)) {
                // logger.warn("Segment " + it + "\n      Size: " + bin.size() +
                // "\n      Mean LOAD~: "
                // + Math.round(bin.mean()) + "\n      Max LOAD~: " +
                // Math.round(bin.max())
                // + "\n      Min LOAD~: " + Math.round(bin.min()) +
                // "\n      Std LOAD~: "
                // + Math.round(bin.standardDeviation()));
                it++;
                double mean = bin.mean() * 1000;
                mean = Math.round(mean);
                mean = mean / 1000.0;
                meanLoad = meanLoad.concat("," + mean);
                segmentLength = segmentLength.concat("," + bin.size());
            }
            // logger.warn(meanRLoad);
            // logger.warn(meanLoad);
            // logger.warn(segmentLength);
        }

        // 9. Store the load factors in the IntermittentTechnologyLoadFactors

        String loadFactors;
        for (Zone zone : zoneList) {
            for (PowerGridNode node : zoneToNodeList.get(zone)) {

                for (PowerGeneratingTechnology technology : technologyList) {
                    String loadFactorString = new String(technology.getName() + " LF in 2 " + node.getName() + ":");
                    logger.warn("Bins for " + zone + ", " + node + "and " + technology);
                    IntermittentTechnologyNodeLoadFactor intTechnologyNodeLoadFactor = reps.intermittentTechnologyNodeLoadFactorRepository
                            .findIntermittentTechnologyNodeLoadFactorForNodeAndTechnology(node, technology);
                    logger.warn("technologyLoadFactor is" + technology + " " + intTechnologyNodeLoadFactor);
                    if (intTechnologyNodeLoadFactor == null) {
                        intTechnologyNodeLoadFactor = new IntermittentTechnologyNodeLoadFactor().persist();
                        intTechnologyNodeLoadFactor.setLoadFactors(new double[noSegments]);
                        intTechnologyNodeLoadFactor.setNode(node);
                        intTechnologyNodeLoadFactor.setTechnology(technology);
                    }

                    it = 1;
                    for (DynamicBin1D bin : loadFactorBinMap.get(zone).get(node).get(technology)) {
                        // logger.warn("Segment " + it + "\n      Size: " +
                        // bin.size() + "\n      Mean RLOAD~: "
                        // + bin.mean() + "\n      Max RLOAD~: " + bin.max() +
                        // "\n      Min RLOAD~: " + bin.min()
                        // + "\n      Std RLOAD~: " + bin.standardDeviation());
                        intTechnologyNodeLoadFactor.setLoadFactorForSegmentId(it, bin.mean());
                        double mean = bin.mean() * 1000000;
                        mean = Math.round(mean);
                        mean = mean / 1000000.0;
                        loadFactorString = loadFactorString.concat(" " + mean);
                        it++;
                    }
                    logger.warn(loadFactorString);
                }

            }
        }

        // 10. Store the segment duration and the average load in that segment
        // per country.

        Iterable<SegmentLoad> segmentLoads = reps.segmentLoadRepository.findAll();
        for (SegmentLoad segmentLoad : segmentLoads) {
            Segment segment = segmentLoad.getSegment();
            Zone zone = segmentLoad.getElectricitySpotMarket().getZone();
            double demandGrowthFactor = reps.marketRepository.findElectricitySpotMarketForZone(zone)
                    .getDemandGrowthTrend().getValue(clearingTick);
            segmentLoad.setBaseLoad(segmentLoadBinsByZone.get(zone)[segment.getSegmentID() - 1].mean()
                    / demandGrowthFactor);
            // logger.warn("Segment " + segment.getSegmentID() + ": " +
            // segmentLoad.getBaseLoad() + "MW");
        }

        Iterable<Segment> segments = reps.segmentRepository.findAll();
        for (Segment segment : segments) {
            segment.setLengthInHours(segmentRloadBins[segment.getSegmentID() - 1].size());
        }

        for (Zone zone : zoneList) {
            double intermittentCapacityOfTechnologyInZone = 0;

            for (PowerGeneratingTechnology technology : technologyList) {
                double productionOfTechInZone = 0;

                for (PowerGridNode node : zoneToNodeList.get(zone)) {

                    intermittentCapacityOfTechnologyInZone += reps.powerPlantRepository
                            .calculateCapacityOfOperationalIntermittentPowerPlantsByPowerGridNodeAndTechnology(node,
                                    technology, getCurrentTick());
                    productionOfTechInZone += m.viewColumn(
                            TECHNOLOGYLOADFACTORSFORZONEANDNODE.get(zone).get(node).get(technology)).zSum()
                            * intermittentCapacityOfTechnologyInZone;

                }
                // logger.warn(technology.getName() + " is producing " +
                // productionOfTechInZone + " MWh in "
                // + zone.getName() + ".");

            }
        }
    }

    public Reps getReps() {
        return reps;
    }

}
