package de.unikn.gabriel.mapfilter;

import org.knime.core.data.StringValue;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;

public class MapFilterNodeDialog extends DefaultNodeSettingsPane {

    public MapFilterNodeDialog() {

        addDialogComponent(new DialogComponentColumnNameSelection(
                MapFilterNodeSettings.createMapColumnNameModel(),
                "Colum in the Mapping Table", MapFilterNodeModel.MAP_PORT,
                StringValue.class));

        addDialogComponent(new DialogComponentColumnNameSelection(
                MapFilterNodeSettings.createFilterColumnNameModel(),
                "Colum in the Filter Table", MapFilterNodeModel.DATA_PORT,
                StringValue.class));
    }

}
