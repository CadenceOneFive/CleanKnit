<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:exslt="http://exslt.org/common" version="1.0" extension-element-prefixes="exslt">
	<xsl:output omit-xml-declaration="yes" indent="no" method="text"/>
	<xsl:template name="combo" match="/">
		<!-- Generate tab-separated values file with a header row. This can be handled without much setup
	by SQLite (via VSV virtual table) -->
		<xsl:text>page_number&#9;top&#9;left&#9;width&#9;height&#9;font&#9;payload&#10;</xsl:text>
		<xsl:for-each select="//page/text">
			<xsl:value-of select="concat(../@number,'&#9;',@top,'&#9;',@left,'&#9;',@width,'&#9;',@height,'&#9;',@font,'&#9;',.,'&#10;')"/>
		</xsl:for-each>
	</xsl:template>
</xsl:stylesheet>
