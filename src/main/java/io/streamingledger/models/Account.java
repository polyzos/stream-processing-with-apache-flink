package io.streamingledger.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {
    private String accountId;
    private int districtId;
    private String frequency;
    private String creationDate;
    private Long updateTime;
}
