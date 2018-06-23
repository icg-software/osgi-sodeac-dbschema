package org.sodeac.dbschema.api;

/**
 * Interface to create default database objects in schema
 * 
 * @author Sebastian Palarus
 *
 */
public interface ISchemaTemplate
{

	/**
	 * implementation of applying schemaTemplate to schemaSpec
	 * 
	 * @param schemaSpec
	 */
	public void schemaTemplateApply(SchemaSpec schemaSpec);
}
