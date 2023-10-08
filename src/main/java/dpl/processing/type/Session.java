package dpl.processing.type;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public enum Session {
    DPL_CASS_PROCESSING("DPL-PROCESSING");

    private String name;

    Session(String name) {
        this.name = name;
    }
}
