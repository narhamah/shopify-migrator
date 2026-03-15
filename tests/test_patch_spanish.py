"""Tests for patch_spanish — Spanish detection heuristics and field scanning."""

import json

import pytest

from tara_migrate.tools.patch_spanish import (
    is_spanish,
    find_spanish_fields,
    _extract_text_from_richtext,
)


# ─────────────────────────────────────────────────────────────────────────────
# is_spanish — Function Word Detection (_ES_FUNCTION_WORDS)
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSpanishFunctionWords:
    """Each function word pattern must trigger detection in a Spanish phrase."""

    def test_de_la(self):
        assert is_spanish("Extracto de la raíz de jengibre")

    def test_del(self):
        assert is_spanish("Cuidado del cuero cabelludo")

    def test_de_los(self):
        assert is_spanish("Beneficios de los ingredientes naturales")

    def test_de_las(self):
        assert is_spanish("Poder de las plantas medicinales")

    def test_para_el(self):
        assert is_spanish("Sérum para el cabello fino")

    def test_para_la(self):
        assert is_spanish("Tratamiento para la caída")

    def test_sin(self):
        assert is_spanish("Fórmula sin sulfatos ni parabenos")

    def test_con_el(self):
        assert is_spanish("Mejora con el uso diario")

    def test_con_la(self):
        assert is_spanish("Combina con la mascarilla nutritiva")

    def test_que_el(self):
        assert is_spanish("Más eficaz que el champú convencional")

    def test_que_la(self):
        assert is_spanish("Mejor que la competencia")

    def test_los(self):
        assert is_spanish("Fortalece los folículos capilares")

    def test_las(self):
        assert is_spanish("Nutre las raíces debilitadas")

    def test_una(self):
        assert is_spanish("Proporciona una hidratación profunda")

    def test_un(self):
        assert is_spanish("Es un tratamiento revolucionario")

    def test_tu(self):
        assert is_spanish("Fortalece tu cabello desde la raíz")

    def test_su(self):
        assert is_spanish("Mejora su densidad y grosor")

    def test_muy(self):
        assert is_spanish("Producto muy eficaz y natural")

    def test_mas(self):
        assert is_spanish("Cabello más fuerte y sano")

    def test_tambien(self):
        assert is_spanish("También ayuda a reducir la caída")

    def test_ademas(self):
        assert is_spanish("Además nutre el folículo capilar")

    def test_sobre(self):
        assert is_spanish("Información sobre nuestros ingredientes")

    def test_entre(self):
        assert is_spanish("Diferencia entre sérum y aceite")

    def test_hacia(self):
        assert is_spanish("Crecimiento hacia la superficie del cuero")

    def test_desde(self):
        assert is_spanish("Fortalecimiento desde la raíz")

    def test_por_el(self):
        assert is_spanish("Reconocido por el sector capilar")

    def test_por_la(self):
        assert is_spanish("Absorbido por la piel rápidamente")

    def test_como(self):
        assert is_spanish("Funciona como barrera protectora")

    # ── Case insensitivity ──

    def test_de_la_uppercase(self):
        assert is_spanish("EXTRACTO DE LA RAÍZ DE JENGIBRE")

    def test_del_mixed_case(self):
        assert is_spanish("Cuidado Del Cuero Cabelludo")

    def test_para_el_upper(self):
        assert is_spanish("SÉRUM PARA EL CABELLO")

    def test_tambien_uppercase(self):
        assert is_spanish("TAMBIÉN FORTALECE EL CABELLO")

    def test_ademas_titlecase(self):
        assert is_spanish("Además Nutre El Folículo")


# ─────────────────────────────────────────────────────────────────────────────
# is_spanish — Domain Word Detection (_ES_DOMAIN_WORDS)
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSpanishDomainWordsOriginal:
    """Original domain words — each tested with a short Spanish phrase."""

    def test_extracto(self):
        assert is_spanish("Extracto natural concentrado")

    def test_aceite(self):
        assert is_spanish("Aceite nutritivo capilar")

    def test_semilla(self):
        assert is_spanish("Semilla orgánica certificada")

    def test_carbon(self):
        assert is_spanish("Carbón activado purificante")

    def test_efecto(self):
        assert is_spanish("Efecto inmediato visible")

    def test_bloqueo(self):
        assert is_spanish("Bloqueo hormonal capilar")

    def test_reactivacion(self):
        assert is_spanish("Reactivación folicular natural")

    def test_calma(self):
        assert is_spanish("Calma folicular profunda")

    def test_densidad(self):
        assert is_spanish("Mayor densidad capilar visible")

    def test_grosor(self):
        assert is_spanish("Aumenta grosor visiblemente")

    def test_raiz(self):
        assert is_spanish("Nutrición desde la raíz")

    def test_estimulacion(self):
        assert is_spanish("Estimulación folicular activa")

    def test_frena(self):
        assert is_spanish("Frena la miniaturización capilar")

    def test_caida(self):
        assert is_spanish("Previene la caída capilar")

    def test_mecanica(self):
        assert is_spanish("Protección mecánica avanzada")

    def test_folicular(self):
        assert is_spanish("Salud folicular óptima")

    def test_visible(self):
        assert is_spanish("Resultado visible inmediato")

    def test_inmediato(self):
        assert is_spanish("Alivio inmediato garantizado")

    def test_olor(self):
        assert is_spanish("Sin olor artificial añadido")

    def test_ancla(self):
        assert is_spanish("Efecto ancla reforzado")

    def test_miniaturizacion(self):
        assert is_spanish("Previene miniaturización progresiva")

    def test_ciclo(self):
        assert is_spanish("Restaura ciclo capilar natural")

    def test_cabello(self):
        assert is_spanish("Fortalece cabello débil")

    def test_cabelludo(self):
        assert is_spanish("Cuero cabelludo equilibrado")

    def test_cuero(self):
        assert is_spanish("Limpieza profunda cuero")

    def test_piel(self):
        assert is_spanish("Suaviza la piel sensible")

    def test_champu(self):
        assert is_spanish("Champú fortificante suave")

    def test_mascarilla(self):
        assert is_spanish("Mascarilla nutritiva intensa")

    def test_acondicionador(self):
        assert is_spanish("Acondicionador reparador premium")

    def test_crema(self):
        assert is_spanish("Crema revitalizante nocturna")

    def test_suavizante(self):
        assert is_spanish("Efecto suavizante duradero")

    def test_proteccion(self):
        assert is_spanish("Protección térmica capilar")

    def test_perdida(self):
        assert is_spanish("Reduce pérdida capilar gradualmente")

    def test_cebolla(self):
        assert is_spanish("Cebolla roja concentrada")

    def test_romero(self):
        assert is_spanish("Aceite esencial romero puro")

    def test_salvia(self):
        assert is_spanish("Extracto salvia orgánica")

    def test_datil(self):
        assert is_spanish("Aceite semilla dátil virgen")

    def test_fresa(self):
        assert is_spanish("Extracto fresa hidratante")

    def test_nopal(self):
        assert is_spanish("Aceite nopal regenerativo")

    def test_aguacate(self):
        assert is_spanish("Aceite aguacate nutritivo")

    def test_apio(self):
        assert is_spanish("Semilla apio estimulante")

    def test_levadura(self):
        assert is_spanish("Levadura fortificante natural")

    def test_oliva(self):
        assert is_spanish("Aceite oliva virgen extra")

    def test_uva(self):
        assert is_spanish("Semilla uva antioxidante")

    def test_sesamo(self):
        assert is_spanish("Aceite sésamo orgánico")

    def test_argan(self):
        assert is_spanish("Aceite argán marroquí")

    def test_soja(self):
        assert is_spanish("Proteína soja hidrolizada")

    def test_crecimiento(self):
        assert is_spanish("Estimula crecimiento capilar")

    def test_capilar(self):
        assert is_spanish("Tratamiento capilar avanzado")

    def test_enjuague(self):
        assert is_spanish("Enjuague suave diario")

    def test_aplicacion(self):
        assert is_spanish("Modo aplicación recomendada")

    def test_resultado(self):
        assert is_spanish("Resultado clínico comprobado")

    def test_beneficio(self):
        assert is_spanish("Beneficio principal: fortalecimiento")

    def test_ingrediente(self):
        assert is_spanish("Ingrediente activo principal")

    def test_tratamiento(self):
        assert is_spanish("Tratamiento intensivo nocturno")


class TestIsSpanishDomainWordsNewForms:
    """NEW word forms: fortalecid[ao], fortalecedor[a], nutritiv[oa], etc."""

    def test_fortalecido(self):
        assert is_spanish("Cabello fortalecido naturalmente")

    def test_fortalecida(self):
        assert is_spanish("Raíz fortalecida desde dentro")

    def test_fortalecedor(self):
        assert is_spanish("Champú fortalecedor orgánico")

    def test_fortalecedora(self):
        assert is_spanish("Rutina fortalecedora completa")

    def test_nutritivo(self):
        assert is_spanish("Aceite nutritivo premium")

    def test_nutritiva(self):
        assert is_spanish("Mascarilla nutritiva intensa")

    def test_reparador(self):
        assert is_spanish("Sérum reparador nocturno")

    def test_reparadora(self):
        assert is_spanish("Crema reparadora profunda")

    def test_vitaminico(self):
        assert is_spanish("Complejo vitamínico capilar")

    def test_vitaminica(self):
        assert is_spanish("Fórmula vitamínica avanzada")


class TestIsSpanishDomainWordsSEO:
    """NEW SEO words: rutina, hidratante, revitalizante, ceramidas, etc."""

    def test_rutina(self):
        assert is_spanish("Rutina completa anticaída")

    def test_hidratante(self):
        assert is_spanish("Sérum hidratante profundo")

    def test_revitalizante(self):
        assert is_spanish("Tratamiento revitalizante nocturno")

    def test_ceramidas(self):
        assert is_spanish("Fórmula con ceramidas activas")

    def test_ajo_negro(self):
        assert is_spanish("Extracto ajo negro concentrado")

    def test_negro(self):
        assert is_spanish("Carbón negro purificante")

    def test_anticaida(self):
        assert is_spanish("Champú anticaída premium")

    def test_limpieza_profunda(self):
        assert is_spanish("Limpieza profunda semanal")

    def test_exfoliante(self):
        assert is_spanish("Exfoliante capilar suave")


# ─────────────────────────────────────────────────────────────────────────────
# is_spanish — Rich Text JSON Handling
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSpanishRichText:
    """is_spanish() on rich_text JSON with Spanish text values."""

    def test_rich_text_with_spanish(self):
        rt = json.dumps({
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "Champú fortalecedor con extracto de romero"}
                ]}
            ]
        })
        assert is_spanish(rt)

    def test_rich_text_with_english(self):
        rt = json.dumps({
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "Strengthening shampoo with rosemary extract"}
                ]}
            ]
        })
        assert not is_spanish(rt)

    def test_rich_text_nested_spanish(self):
        rt = json.dumps({
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "English text here"},
                ]},
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "Tratamiento para la caída capilar"},
                ]},
            ]
        })
        assert is_spanish(rt)

    def test_rich_text_short_spanish_values_skipped(self):
        """Text values <= 5 chars are skipped in rich text scanning."""
        rt = json.dumps({
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "un"}  # <= 5 chars, skipped
                ]}
            ]
        })
        assert not is_spanish(rt)

    def test_rich_text_invalid_json_fallback(self):
        """If JSON is invalid, falls through to normal text detection."""
        text = '{"type": broken json with champú fortalecedor'
        # Starts with {"type": but invalid JSON — falls back to text check
        assert is_spanish(text)

    def test_rich_text_multiple_text_nodes(self):
        rt = json.dumps({
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "Hello world"},
                    {"type": "text", "value": "Good morning"},
                ]},
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "Aceite nutritivo capilar profundo"},
                ]},
            ]
        })
        assert is_spanish(rt)


# ─────────────────────────────────────────────────────────────────────────────
# is_spanish — Edge Cases
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSpanishEdgeCases:
    """Short strings, empty/None, English, brand names, INCI names, mixed text."""

    # ── Short strings ──

    def test_short_string_1_char(self):
        assert not is_spanish("a")

    def test_short_string_2_chars(self):
        assert not is_spanish("OK")

    def test_short_string_3_chars(self):
        assert not is_spanish("abc")

    def test_exactly_4_chars_no_spanish(self):
        """4 chars is the minimum — but only triggers if Spanish words are found."""
        assert not is_spanish("test")

    # ── Empty / None ──

    def test_empty_string(self):
        assert not is_spanish("")

    def test_none(self):
        assert not is_spanish(None)

    def test_non_string_int(self):
        assert not is_spanish(42)

    def test_non_string_list(self):
        assert not is_spanish(["extracto"])

    # ── Pure English ──

    def test_pure_english_sentence(self):
        assert not is_spanish("This is a high-quality hair care product")

    def test_pure_english_product_description(self):
        assert not is_spanish(
            "Our advanced formula strengthens hair follicles and prevents breakage"
        )

    def test_pure_english_with_numbers(self):
        assert not is_spanish("Apply 2-3 drops daily for best results")

    # ── Brand names ──

    def test_brand_tara(self):
        assert not is_spanish("TARA")

    def test_brand_tara_in_sentence(self):
        assert not is_spanish("TARA scalp care system")

    def test_brand_kansa_wand(self):
        assert not is_spanish("Kansa Wand")

    def test_brand_kansa_wand_pro(self):
        assert not is_spanish("TARA Kansa Wand Pro")

    def test_brand_gua_sha(self):
        assert not is_spanish("Gua Sha facial tool")

    # ── INCI names ──

    def test_inci_single(self):
        assert not is_spanish("Sodium Lauryl Sulfate")

    def test_inci_list(self):
        assert not is_spanish(
            "Aqua, Cetearyl Alcohol, Glycerin, Tocopherol, Panthenol"
        )

    def test_inci_with_parentheses(self):
        assert not is_spanish(
            "Prunus Amygdalus Dulcis (Sweet Almond) Oil"
        )

    # ── Mixed English-Spanish ──

    def test_mixed_should_detect_spanish(self):
        assert is_spanish(
            "Advanced treatment para el cuero cabelludo with biotin"
        )

    def test_mixed_spanish_word_in_english(self):
        assert is_spanish(
            "This champú contains natural ingredients for strong hair"
        )


# ─────────────────────────────────────────────────────────────────────────────
# is_spanish — English must NOT false-positive
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSpanishNoFalsePositives:
    """Function words must NOT false-positive on English text."""

    def test_english_with_the(self):
        assert not is_spanish("The best hair care solution for you")

    def test_english_with_for(self):
        assert not is_spanish("Made for sensitive scalps")

    def test_english_with_from(self):
        assert not is_spanish("Extracted from organic sources")

    def test_english_with_our(self):
        assert not is_spanish("Our premium hair serum collection")

    def test_english_with_your(self):
        assert not is_spanish("Transform your hair routine today")

    def test_english_skincare(self):
        assert not is_spanish("Gentle cleansing foam for daily use")

    def test_english_scientific(self):
        assert not is_spanish("Clinically tested and dermatologically approved")

    def test_english_marketing(self):
        assert not is_spanish("Award-winning formula with 97% natural ingredients")

    def test_english_instructions(self):
        assert not is_spanish(
            "Apply a small amount to wet hair, massage gently, and rinse thoroughly"
        )

    def test_english_benefits_list(self):
        assert not is_spanish(
            "Strengthens roots, reduces breakage, adds volume and shine"
        )

    def test_english_with_numbers_and_units(self):
        assert not is_spanish("200ml bottle, lasts approximately 2 months")

    def test_english_complex_sentence(self):
        assert not is_spanish(
            "Our patented bio-complex delivers nutrients directly to the hair "
            "follicle, promoting growth and reducing shedding within 8 weeks"
        )


# ─────────────────────────────────────────────────────────────────────────────
# is_spanish — Real-World Spanish Meta Titles (Previously Escaped Detection)
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSpanishRealWorldSpanishTitles:
    """Real meta_titles that previously escaped detection — MUST return True."""

    def test_rutina_reparadora_fortalecedora(self):
        assert is_spanish(
            "Rutina Reparadora y Fortalecedora con Ajo Negro y Ceramidas | TARA"
        )

    def test_acondicionador_hidratante_fresa(self):
        assert is_spanish(
            "Acondicionador Hidratante Fresa + NMF | TARA"
        )

    def test_acondicionador_suavizante_nutritivo(self):
        assert is_spanish(
            "Acondicionador Suavizante y Nutritivo | TARA"
        )


# ─────────────────────────────────────────────────────────────────────────────
# is_spanish — Real-World English Text (Must NOT Be Detected)
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSpanishRealWorldEnglish:
    """Real English product names/titles — MUST return False."""

    def test_rejuvenating_scalp_serum(self):
        assert not is_spanish("Rejuvenating Scalp Serum")

    def test_hair_wellness_system(self):
        assert not is_spanish("Hair wellness system")

    def test_replenishing_conditioner(self):
        assert not is_spanish("Replenishing Conditioner")

    def test_nurture_leave_in_conditioner(self):
        assert not is_spanish("Nurture Leave-In Conditioner")

    def test_tara_kansa_wand_pro(self):
        assert not is_spanish("TARA Kansa Wand Pro")

    def test_deep_cleansing_clay_mask(self):
        assert not is_spanish("Deep Cleansing Clay Mask")


# ─────────────────────────────────────────────────────────────────────────────
# _extract_text_from_richtext — helper
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractTextFromRichtext:
    def test_simple(self):
        node = {
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "Hello world"}
                ]}
            ]
        }
        assert _extract_text_from_richtext(node) == ["Hello world"]

    def test_multiple_text_nodes(self):
        node = {
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [
                    {"type": "text", "value": "First"},
                    {"type": "text", "value": "Second"},
                ]},
            ]
        }
        result = _extract_text_from_richtext(node)
        assert "First" in result
        assert "Second" in result

    def test_nested(self):
        node = {
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [
                    {"type": "list", "children": [
                        {"type": "list-item", "children": [
                            {"type": "text", "value": "Nested text"}
                        ]}
                    ]}
                ]}
            ]
        }
        assert "Nested text" in _extract_text_from_richtext(node)

    def test_empty_node(self):
        assert _extract_text_from_richtext({}) == []

    def test_list_input(self):
        nodes = [
            {"type": "text", "value": "Item 1"},
            {"type": "text", "value": "Item 2"},
        ]
        result = _extract_text_from_richtext(nodes)
        assert "Item 1" in result
        assert "Item 2" in result

    def test_no_text_nodes(self):
        node = {"type": "root", "children": [{"type": "paragraph", "children": []}]}
        assert _extract_text_from_richtext(node) == []


# ─────────────────────────────────────────────────────────────────────────────
# find_spanish_fields
# ─────────────────────────────────────────────────────────────────────────────

class TestFindSpanishFields:
    def test_finds_spanish_values(self):
        progress = {
            "product.title": "Champú fortalecedor con extracto",
            "product.body": "Strengthening shampoo with extract",
        }
        result = find_spanish_fields(progress)
        assert "product.title" in result
        assert "product.body" not in result

    def test_skips_handle_fields(self):
        progress = {
            "product.handle": "champu-fortalecedor-con-extracto",
        }
        result = find_spanish_fields(progress)
        assert len(result) == 0

    def test_skips_inci_name_fields(self):
        progress = {
            "mo.ingredient.abc.inci_name": "Olea Europaea Fruit Oil",
        }
        result = find_spanish_fields(progress)
        assert len(result) == 0

    def test_skips_non_string_values(self):
        progress = {
            "product.id": 12345,
            "product.active": True,
            "product.tags": ["cabello", "cuidado"],
        }
        result = find_spanish_fields(progress)
        assert len(result) == 0

    def test_empty_progress(self):
        assert find_spanish_fields({}) == {}

    def test_multiple_spanish_fields(self):
        progress = {
            "product.title": "Champú fortalecedor",
            "product.tagline": "Tratamiento capilar avanzado",
            "product.body": "Advanced hair treatment",
            "product.handle": "champu-fortalecedor",
            "mo.ingredient.xyz.inci_name": "Aqua",
        }
        result = find_spanish_fields(progress)
        assert len(result) == 2
        assert "product.title" in result
        assert "product.tagline" in result

    def test_returns_values(self):
        """The returned dict maps key -> original value."""
        progress = {
            "product.title": "Extracto natural concentrado",
        }
        result = find_spanish_fields(progress)
        assert result["product.title"] == "Extracto natural concentrado"
