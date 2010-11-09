<?xml version="1.0" encoding="utf-8"?>
<!-- 
    Thanks are due to the nuxeo.org team who put together this stylesheet.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format"
                version="1.0">

    <xsl:import href="urn:docbkx:stylesheet"/>

<xsl:param name="local.l10n.xml" select="document('')"/>
<l:i18n xmlns:l="http://docbook.sourceforge.net/xmlns/l10n/1.0">
  <l:l10n language="en">
    <l:context name="xref-number-and-title">
      <l:template name="section" style="table" text="%t"/>
     </l:context>
  </l:l10n>
</l:i18n>

    <!--###################################################
                    HTML Settings
   ################################################### -->

    <xsl:param name="html.stylesheet">html.css</xsl:param>

    <!-- These extensions are required for table printing and other stuff -->
    <xsl:param name="use.extensions">1</xsl:param>
    <xsl:param name="tablecolumns.extension">1</xsl:param>
    <xsl:param name="callout.extension">1</xsl:param>
    <xsl:param name="graphicsize.extension">0</xsl:param>
    <xsl:param name="keep.relative.image.uris" select="1"></xsl:param>
    
    <!--###################################################
                     Table Of Contents
   ################################################### -->

    <!-- Generate the TOCs for named components only -->
    <xsl:param name="generate.toc">
      book toc,title
      chapter toc
      article/appendix toc
      qandadiv nop
      qandaset toc
    </xsl:param>

    <!-- Show only Sections up to level 2 in the TOCs -->
    <xsl:param name="toc.section.depth">3</xsl:param>
    <xsl:param name="generate.section.toc.level" select="0"></xsl:param>

    <!--###################################################
                        Labels
   ################################################### -->

    <!-- Label Chapters and Sections (numbering) -->
    <xsl:param name="chapter.autolabel">1</xsl:param>
    <xsl:param name="chapter.label.includes.component.label">1</xsl:param>
    <xsl:param name="section.autolabel">1</xsl:param>
    <xsl:param name="section.label.includes.component.label">1</xsl:param>
    <xsl:param name="section.autolabel.max.depth">3</xsl:param>

    <!--###################################################
                        Callouts
   ################################################### -->

    <!-- Use images for callouts instead of (1) (2) (3) -->
    <xsl:param name="callout.graphics">1</xsl:param>

    <!-- Place callout marks at this column in annotated areas -->
    <xsl:param name="callout.defaultcolumn">90</xsl:param>

    <!--###################################################
                      Admonitions
   ################################################### -->

    <!-- Use nice graphics for admonitions -->
    <xsl:param name="admon.graphics">0</xsl:param>

    <!--###################################################
                         Misc
   ################################################### -->
    <!-- Placement of titles -->
    <xsl:param name="formal.title.placement">
      figure after
      example after
      equation after
      table after
      procedure after
    </xsl:param>
    <xsl:template match="author" mode="titlepage.mode">
        <xsl:if test="name(preceding-sibling::*[1]) = 'author'">
            <xsl:text>, </xsl:text>
        </xsl:if>
        <span class="{name(.)}">
            <xsl:call-template name="person.name"/>
            <xsl:apply-templates mode="titlepage.mode" select="./contrib"/>
            <xsl:apply-templates mode="titlepage.mode" select="./affiliation"/>
        </span>
    </xsl:template>
    <xsl:template match="authorgroup" mode="titlepage.mode">
        <div class="{name(.)}">
            <h2>Authors</h2>
            <p/>
            <xsl:apply-templates mode="titlepage.mode"/>
        </div>
    </xsl:template>
    
    <!--###################################################
                     Headers and Footers
    ################################################### -->
    <xsl:template name="user.header.content">      
        <div id="banner">
            <a style="border:none;" href="http://www.symmetricds.org/"
               title="SymmetricDS User Guide">
                <img style="border:none;" alt="SymmetricDS"
                  src="images/banner-logo.gif"/>
            </a>
        </div>
    </xsl:template>
    
    <xsl:template name="user.footer.content">
      <p class="copyright">&#x00A9; 2007, 2008 Eric Long and Chris Henson</p>
    </xsl:template>

</xsl:stylesheet>
