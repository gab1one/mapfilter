package de.unikn.gabriel.mapfilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnName;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;

public class MapFilterNodeModel extends NodeModel {

    static final int DATA_PORT = 0;
    static final int MAP_PORT = 1;
    private static final String PRESENT = "1";

    private Map<String, String> m_referenceMap;
    private final List<SettingsModel> m_settingsmodels = new ArrayList<>();

    private final SettingsModelColumnName m_mapColNameModel =
            MapFilterNodeSettings.createMapColumnNameModel();
    private final SettingsModelColumnName m_filterColNameModel =
            MapFilterNodeSettings.createFilterColumnNameModel();

    private int m_mapColidx;
    private int m_filterColidx;

    public MapFilterNodeModel() {
        super(2, 1);
        m_settingsmodels.add(m_mapColNameModel);
        m_settingsmodels.add(m_filterColNameModel);
    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
            throws InvalidSettingsException {

        m_mapColidx = inSpecs[MAP_PORT]
                .findColumnIndex(m_mapColNameModel.getStringValue());

        m_filterColidx = inSpecs[DATA_PORT]
                .findColumnIndex(m_filterColNameModel.getStringValue());

        return new DataTableSpec[] { inSpecs[DATA_PORT] };
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws Exception {

        m_referenceMap = new HashMap<>();
        /**
         * {@inheritDoc}
         */

        final boolean useMapRowID = m_mapColNameModel.getStringValue() == null;
        // Fill reference map
        inData[MAP_PORT].forEach(row -> {
            final String key = useMapRowID ? row.getKey().getString()
                    : ((StringValue) row.getCell(m_filterColidx))
                            .getStringValue();
            m_referenceMap.put(key, PRESENT);
        });

        final BufferedDataContainer container =
                exec.createDataContainer(inData[DATA_PORT].getDataTableSpec());

        final boolean useFilterRowID =
                m_filterColNameModel.getStringValue() == null;

        inData[DATA_PORT].forEach(row -> {
            final String key = useFilterRowID ? row.getKey().getString()
                    : ((StringValue) row.getCell(m_filterColidx))
                            .getStringValue();
            if (m_referenceMap.containsKey(key)) {
                container.addRowToTable(row);
            }
        });
        container.close();
        return new BufferedDataTable[] { container.getTable() };
    }

    // --- streaming ---

    @Override
    public StreamableOperator createStreamableOperator(
            final PartitionInfo partitionInfo, final PortObjectSpec[] inSpecs)
                    throws InvalidSettingsException {

        return new StreamableOperator() {

            @Override
            public void runFinal(final PortInput[] inputs,
                    final PortOutput[] outputs, final ExecutionContext exec)
                            throws Exception {

                m_referenceMap = new HashMap<>();
                final RowInput dataRowInput = (RowInput) inputs[DATA_PORT];
                final RowInput mapRowInput = (RowInput) inputs[MAP_PORT];

                final RowOutput rowOutput = (RowOutput) outputs[0];

                // Fill lookup map
                final boolean useMapRowID =
                        m_mapColNameModel.getStringValue() == null;
                DataRow inputRow;
                exec.setMessage("Creating lookup table!");
                while ((inputRow = mapRowInput.poll()) != null) {
                    final String key =
                            useMapRowID ? inputRow.getKey().getString()
                                    : ((StringValue) inputRow
                                            .getCell(m_filterColidx))
                                                    .getStringValue();
                    m_referenceMap.put(key, PRESENT);
                }
                mapRowInput.close();

                // filtering
                final boolean useFilterRowID =
                        m_filterColNameModel.getStringValue() == null;
                long index = 0;
                while ((inputRow = dataRowInput.poll()) != null) {
                    final String key =
                            useFilterRowID ? inputRow.getKey().getString()
                                    : ((StringValue) inputRow
                                            .getCell(m_filterColidx))
                                                    .getStringValue();
                    if (m_referenceMap.containsKey(key)) {
                        rowOutput.push(inputRow);
                    }
                    exec.setMessage(String.format("Row %d (\"%s\"))", ++index,
                            inputRow.getKey()));
                }
                dataRowInput.close();
                rowOutput.close();
            }
        };
    }

    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[] { InputPortRole.DISTRIBUTED_STREAMABLE,
                InputPortRole.DISTRIBUTED_STREAMABLE };
    }

    @Override
    public OutputPortRole[] getOutputPortRoles() {
        return new OutputPortRole[] { OutputPortRole.DISTRIBUTED };
    }

    @Override
    protected void reset() {
        m_referenceMap = null;
    }

    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
                    throws IOException, CanceledExecutionException {
        // Not needed
    }

    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
                    throws IOException, CanceledExecutionException {
        // Not needed
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        for (final SettingsModel sm : m_settingsmodels) {
            sm.saveSettingsTo(settings);
        }
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {

        for (final SettingsModel sm : m_settingsmodels) {
            sm.validateSettings(settings);
        }
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        for (final SettingsModel sm : m_settingsmodels) {
            sm.loadSettingsFrom(settings);
        }

    }

}
