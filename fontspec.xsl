<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:exslt="http://exslt.org/common" version="1.0" extension-element-prefixes="exslt">
	<xsl:output omit-xml-declaration="yes" indent="no" method="text"/>
	<xsl:template name="combo" match="/">
	  <xsl:text>page_number&#9;id&#9;size&#9;family&#9;color&#10;</xsl:text>
	  <xsl:for-each select="//page/fontspec">
	     <xsl:value-of select="concat(../@number,'&#9;',@id,'&#9;',@size,'&#9;',@family,'&#9;',@color,'&#10;')"/>
          </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>
