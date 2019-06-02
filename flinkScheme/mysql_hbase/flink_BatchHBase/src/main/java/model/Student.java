package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * by 光城
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    public String database;
    public String table;
    public String type;
    public int ts;
    public int xid;
    public Boolean commit;
    public String data;

}
