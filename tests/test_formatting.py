from markdownify import markdownify as md

html1 = """
<div class="content"> <div class="rich--text"> <h5 class=""> Rechtsgrundlage</h5> <p> Richtlinie zur Förderung der Gesundheit und Robustheit landwirtschaftlicher Nutztiere (NuTieFöRL M-V)<strong><br> </strong><span>vom: 20.08.2022 – VI 320-2<br> Verwaltungsvorschrift des Ministeriums für Klimaschutz, Landwirtschaft, ländliche Räume und Umwelt<br> </span><span><abbr class="" title="Verwaltungsvorschrift">VV</abbr> Meckl.-Vorp. Gl.-<abbr class="" title="Nummer">Nr.</abbr> 630 - 420<br> Fundstelle: AmtsBl. M-V 2022 <abbr class="" title="Seite">S.</abbr> 548<br> </span></p> <p> <span><span>Der Antrag ist vollständig bis zum 30. November für das jeweils folgende Jahr bei der Bewilligungsbehörde einzureichen.</span></span></p> <p> <span><span><a class="RichTextExtLink ExternalLink" href="https://www.landesrecht-mv.de/bsmv/document/VVMV-VVMV000010894" title="Richtlinie zur Förderung der Gesundheit und Robustheit landwirtschaftlicher Nutztiere (NuTieFöRL M-V) vom: 20.08.2022 – VI 320-2 Verwaltungsvorschrift des Ministeriums für Klimaschutz, Landwirtschaft, ländliche Räume und Umwelt VV Meckl.-Vorp. Gl.-Nr. 630 - 420 Fundstelle: AmtsBl. M-V 2022 S. 548">Weblink zur Förderrichtlinie</a></span></span></p> </div> </div>
"""

html2 = """
<article class="content--tab-text"> <div class="rich--text"> <h3>Kurztext</h3> <p> Wenn Sie Vorhaben durchführen, mit denen die beruflichen Kompetenzen von Beschäftigten gesteigert werden, können Sie unter bestimmten Voraussetzungen eine Förderung erhalten.</p> </div> <div class="rich--text"> <h3>Volltext</h3> <p> Das Land Mecklenburg-Vorpommern unterstützt Sie mit Mitteln des Europäischen Sozialfonds Plus (<abbr class="" title="">ESF+</abbr>) bei Vorhaben der berufsbegleitenden Qualifizierung.</p> <p> Sie erhalten die Förderung für</p> <ul> <li>die Teilnahme von Beschäftigten an der beruflichen Weiterbildung durch Bildungsschecks sowie</li><li>unternehmensspezifische Maßnahmen zur Kompetenzfeststellung der Beschäftigten, zur Analyse des Qualifizierungsbedarfs von Beschäftigten bezüglich deren Arbeitsplätze im Unternehmen und zur beruflichen Qualifizierung.</li></ul> <p> Sie erhalten die Förderung als Zuschuss.</p> <p> Die Höhe des Zuschusses beträgt für</p> <ul> <li>für Bildungsschecks 50 Prozent der zuwendungsfähigen Ausgaben, maximal jedoch <abbr class="" title="Euro">EUR</abbr> 3.000 je Bildungsscheck und Qualifizierungsmaßnahme und</li><li>für unternehmensspezifische Maßnahmen grundsätzlich 50 Prozent der zuwendungsfähigen Ausgaben; für mittlere Unternehmen kann die Höhe der Förderung 60 Prozent und für kleine Unternehmen 70 Prozent der zuwendungsfähigen Ausgaben betragen.</li></ul> <p> Stellen Sie Ihren Antrag bitte vor Beginn der zu fördernden Maßnahme bei der Gesellschaft für Struktur- und Arbeitsmarktentwicklung (GSA).</p> </div> </article>
"""


def test_format():
    print(md(html1))
    print("\n\n")
    print(md(html2))
