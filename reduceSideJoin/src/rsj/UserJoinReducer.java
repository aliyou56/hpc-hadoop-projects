package rsj;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    // Get the type of join from our configuration
    public void setup(Context context){
        joinType = context.getConfiguration().get("join.type");
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listA.clear();
        listB.clear();
        for (Text t : values){// bin each record based on what it was tagged with
            if (t.charAt(0) == 'A')
                listA.add(new Text(t.toString().substring(1)));
            else if (t.charAt(0) == 'B')
                listB.add(new Text(t.toString().substring(1)));
        }
        // Execute our join logic now that the lists are filled
        executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException,InterruptedException {
        switch(joinType) {
            case "inner" : // If both lists are not empty, join A with B
                if (!listA.isEmpty() && !listB.isEmpty())
                    for (Text A : listA)
                        for (Text B : listB) context.write(A, B);
                break;
            case "left" : // For each entry in A,
                for (Text A : listA) // If list B is not empty, join A and B
                    if (!listB.isEmpty())
                        for (Text B : listB) context.write(A, B);
                    else // Else, output A by itself
                        context.write(A, new Text(""));
                break;
            case "right" :// For each entry in B,
                for (Text B : listB) // If list A is not empty, join A and B
                    if (!listA.isEmpty())
                        for (Text A : listA) context.write(A, B);
                    else // Else, output B by itself
                        context.write(new Text(""), B);
                break;
            case "full" : // If list A is not empty
                if (!listA.isEmpty()) // For each entry in A
                    for (Text A : listA) // If list B is not empty, join A with B
                        if (!listB.isEmpty())
                            for (Text B : listB) context.write(A, B);
                        else // Else, output A by itself
                            context.write(A, new Text(""));
                else // If list A is empty, just output B
                    for (Text B : listB) context.write(new Text(""), B);
                break;
            case "anti" : // If list A is empty or B is empty or vice versa
                if (listA.isEmpty() ^ listB.isEmpty()) {
                    for (Text A : listA) context.write(A, new Text(""));
                    for (Text B : listB) context.write(new Text(""), B);
                }
                break;
            default :
                throw new RuntimeException("Join type set incorrectly");
        }
    }
}