part of mongo_dart;

mixin BulkCommand {
	List<Section> getSections();

	Map<String, dynamic> asSingleSectionPayload() {
		var sections = getSections();
		var command = (sections[0] as MainSection).payload.data;
		sections.skip(1).forEach((sec) => (sec as PayloadSection).asMapElement(command));
		return command;
	}
}
