package de.unikn.gabriel.mapfilter;

import org.knime.core.node.defaultnodesettings.SettingsModelColumnName;

public class MapFilterNodeSettings {

    private MapFilterNodeSettings() {
        // Util method
    }

    static SettingsModelColumnName createMapColumnNameModel() {
        return new SettingsModelColumnName("map column name", "");
    }

    static SettingsModelColumnName createFilterColumnNameModel() {
        return new SettingsModelColumnName("filter column name", "");
    }

}
