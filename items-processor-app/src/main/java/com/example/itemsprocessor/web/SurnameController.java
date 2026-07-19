package com.example.itemsprocessor.web;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class SurnameController {
    private static final Map<String, String> TRANSLATIONS = Map.of(
            "oak", "Chene", "elm", "Orme", "birch", "Bouleau", "rowan", "Sorbier",
            "juniper", "Genevrier", "sycamore", "Sycomore", "kukui", "Bancoulier",
            "magnolia", "Magnolia", "willow", "Saule", "maple", "Erable");

    @GetMapping("/health") public Map<String, String> health() { return Map.of("status", "ok"); }
    @GetMapping("/surname/{surname}/french") public Translation byPath(@PathVariable String surname) { return translate(surname); }
    @GetMapping("/translate") public Translation byQuery(@RequestParam String surname) { return translate(surname); }

    private Translation translate(String surname) {
        String french = TRANSLATIONS.get(surname.trim().toLowerCase());
        if (french == null) throw new TranslationNotFound(surname);
        return new Translation(surname, french);
    }
    public record Translation(String surname, String frenchName) { }
    @ResponseStatus(HttpStatus.NOT_FOUND)
    private static final class TranslationNotFound extends RuntimeException {
        private TranslationNotFound(String surname) { super("No French name configured for surname '" + surname + "'"); }
    }
}
