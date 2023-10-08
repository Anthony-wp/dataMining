package dpl.processing.type;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Session {
    DPL_CASS_PROCESSING("DPL-PROCESSING");

    private final String name;
}
