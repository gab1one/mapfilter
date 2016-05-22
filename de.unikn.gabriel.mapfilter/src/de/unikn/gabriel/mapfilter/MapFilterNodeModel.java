package de.unikn.gabriel.mapfilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
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

public class MapFilterNodeModel extends NodeModel {

    private static final int DATA_PORT = 0;
    private static final int MAP_PORT = 1;
    private static final String PRESENT = "1";

    private Map<String, String> m_referenceMap;
    private final List<SettingsModel> m_settingsmodels = new ArrayList<>();

    private final SettingsModelColumnName m_mappingColNameModel =
            new SettingsModelColumnName("mapping column name", "");

    private final SettingsModelColumnName m_filterColNameModel =
            new SettingsModelColumnName("filter column name", "");

    private int m_mapColidx;
    private int m_filterColidx;

    public MapFilterNodeModel() {
        super(2, 1);
        m_settingsmodels.add(m_mappingColNameModel);
    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
            throws InvalidSettingsException {

        m_mapColidx = inSpecs[MAP_PORT]
                .findColumnIndex(m_mappingColNameModel.getStringValue());

        m_filterColidx = inSpecs[DATA_PORT]
                .findColumnIndex(m_filterColNameModel.getStringValue());

        return new DataTableSpec[] { inSpecs[DATA_PORT] };
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws Exception {

        m_referenceMap = new HashMap<>();

        // Fill reference map
        inData[MAP_PORT].forEach(row -> m_referenceMap.put(
                ((StringValue) row.getCell(m_mapColidx)).getStringValue(),
                PRESENT));

        inData[DATA_PORT].forEach(row -> {
            if (m_referenceMap.get(((StringValue) row.getCell(m_filterColidx))
                    .getStringValue()) != null) {

            }

        });

        return null;
    }

    @Override
    protected void reset() {
        m_referenceMap = null;
    }

    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
                    throws IOException, CanceledExecutionException {
    }

    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
                    throws IOException, CanceledExecutionException {
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
