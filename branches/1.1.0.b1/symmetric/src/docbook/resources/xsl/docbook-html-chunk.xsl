<?xml version="1.0" encoding="utf-8"?>
<!-- 
    Thanks are due to the nuxeo.org team who put together this stylesheet.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format"
                version="1.0">
    
    <xsl:import href="urn:docbkx:stylesheet"/>
    
    <!--###################################################
                     HTML Settings
    ################################################### -->
    <xsl:param name="chunk.section.depth">0</xsl:param>
    <xsl:param name="use.id.as.filename">1</xsl:param>
    
    <!-- Graphics location definition -->
    <xsl:param name="keep.relative.image.uris" select="1"></xsl:param>
    <xsl:param name="img.src.path">./</xsl:param>
    
    <!-- These extensions are required for table printing and other stuff -->
    <xsl:param name="use.extensions">1</xsl:param>
    <xsl:param name="tablecolumns.extension">1</xsl:param>
    <xsl:param name="callout.extension">1</xsl:param>
    <xsl:param name="graphicsize.extension">0</xsl:param>
    
    <!--###################################################
                      Table Of Contents
    ################################################### -->

    <!-- Generate the TOCs for named components only -->
    <xsl:param name="generate.toc">
      book toc,title
      chapter toc,title
      book/section toc,title
      qandadiv nop
      qandaset toc  
    </xsl:param>
    
    <!-- Show only Sections up to level 3 in the TOCs -->
    <xsl:param name="toc.section.depth">2</xsl:param>
 
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

    <!-- Place callout marks at this column in annotated areas -->
    <xsl:param name="callout.graphics">1</xsl:param>
    <xsl:param name="callout.defaultcolumn">90</xsl:param>

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

    <xsl:template name="user.header.navigation">      
        <div id="banner">
            <a style="border:none;" href="http://www.symmetricds.org/"
               title="SymmetricDS User Guide">
                <img style="border:none;"
                  width="455" height="69" alt="SymmetricDS"
                  src="images/banner_logo.gif"/>
            </a>
        </div>
    </xsl:template>
    <!-- no other header navigation (prev, next, etc.) -->
    <xsl:template name="header.navigation"/>
    <xsl:param name="navig.showtitles">1</xsl:param>

    <xsl:template name="footer.navigation">
        <xsl:param name="prev" select="/foo"/>
        <xsl:param name="next" select="/foo"/>
        <xsl:param name="nav.context"/>
        <xsl:variable name="home" select="/*[1]"/>
        <xsl:variable name="up" select="parent::*"/>
        <xsl:variable name="row1" select="count($prev) &gt; 0
                                        or count($up) &gt; 0
                                        or count($next) &gt; 0"/>
        <xsl:variable name="row2" select="($prev and $navig.showtitles != 0)
                                        or (generate-id($home) != generate-id(.)
                                            or $nav.context = 'toc')
                                        or ($chunk.tocs.and.lots != 0
                                            and $nav.context != 'toc')
                                        or ($next and $navig.showtitles != 0)"/>
        <xsl:if test="$suppress.navigation = '0' and $suppress.footer.navigation = '0'">
            <div class="navfooter">
                <xsl:if test="$footer.rule != 0">
                    <hr/>
                </xsl:if>
                <xsl:if test="$row1 or $row2">
                    <table width="100%" summary="Navigation footer">
                        <xsl:if test="$row1">
                            <tr>
                                <td width="30%" align="left">
                                    <xsl:if test="count($prev)>0">
                                        <a accesskey="p">
                                            <xsl:attribute name="href">
                                                <xsl:call-template name="href.target">
                                                    <xsl:with-param name="object" select="$prev"/>
                                                </xsl:call-template>
                                            </xsl:attribute>
                                            <xsl:call-template name="navig.content">
                                                <xsl:with-param name="direction" select="'prev'"/>
                                            </xsl:call-template>
                                        </a>
                                    </xsl:if>
                                    <xsl:text>&#160;</xsl:text>
                                </td>

                                <td width="40%" align="center">
                                    <xsl:choose>
                                        <xsl:when test="$home != . or $nav.context = 'toc'">
                                            <a accesskey="h">
                                                <xsl:attribute name="href">
                                                    <xsl:call-template name="href.target">
                                                        <xsl:with-param name="object" select="$home"/>
                                                    </xsl:call-template>
                                                </xsl:attribute>
                                                <xsl:call-template name="navig.content">
                                                    <xsl:with-param name="direction" select="'home'"/>
                                                </xsl:call-template>
                                            </a>
                                            <xsl:if test="$chunk.tocs.and.lots != 0 and $nav.context != 'toc'">
                                                <xsl:text>&#160;|&#160;</xsl:text>
                                            </xsl:if>
                                        </xsl:when>
                                        <xsl:otherwise>&#160;</xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:if test="$chunk.tocs.and.lots != 0 and $nav.context != 'toc'">
                                        <a accesskey="t">
                                            <xsl:attribute name="href">
                                                <xsl:apply-templates select="/*[1]" mode="recursive-chunk-filename">
                                                    <xsl:with-param name="recursive" select="true()"/>
                                                </xsl:apply-templates>
                                                <xsl:text>-toc</xsl:text>
                                                <xsl:value-of select="$html.ext"/>
                                            </xsl:attribute>
                                            <xsl:call-template name="gentext">
                                                <xsl:with-param name="key" select="'nav-toc'"/>
                                            </xsl:call-template>
                                        </a>
                                    </xsl:if>
                                </td>
                                <td width="30%" align="right">
                                    <xsl:text>&#160;</xsl:text>
                                    <xsl:if test="count($next)>0">
                                        <a accesskey="n">
                                            <xsl:attribute name="href">
                                                <xsl:call-template name="href.target">
                                                    <xsl:with-param name="object" select="$next"/>
                                                </xsl:call-template>
                                            </xsl:attribute>
                                            <xsl:call-template name="navig.content">
                                                <xsl:with-param name="direction" select="'next'"/>
                                            </xsl:call-template>
                                        </a>
                                    </xsl:if>
                                </td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="$row2">
                            <tr>
                                <td width="30%" align="left" valign="top">
                                    <xsl:if test="$navig.showtitles != 0">
                                        <xsl:apply-templates select="$prev" mode="object.title.markup"/>
                                    </xsl:if>
                                    <xsl:text>&#160;</xsl:text>
                                </td>
                                <td width="40%" align="center">
                                    <span style="color:white;font-size:90%;">
                                        <a href="http://www.symmetricds.org/"
                                           title="SymmetricDS">SymmetricDS
                                        </a>
                                    </span>
                                </td>
                                <td width="30%" align="right" valign="top">
                                    <xsl:text>&#160;</xsl:text>
                                    <xsl:if test="$navig.showtitles != 0">
                                        <xsl:apply-templates select="$next" mode="object.title.markup"/>
                                    </xsl:if>
                                </td>
                            </tr>
                        </xsl:if>
                    </table>
                </xsl:if>
            </div>
        </xsl:if>
    </xsl:template>
    
    <xsl:template name="user.footer.content">
    </xsl:template>
    
</xsl:stylesheet>
