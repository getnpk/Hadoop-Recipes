import java.io.*;

import javax.swing.*;
import java.net.*;
import java.awt.*;
import java.awt.event.*;

public class HadoopClient {

    JTextArea output;
    JTextArea querry;
    JButton sendbutton;
   
    JProgressBar progressBar;
   
    BufferedReader reader;
    PrintWriter writer;
   
    Socket sock;

    Font font_querry;
    Font font_output;
   
    public void go(){
        JFrame frame = new JFrame("Client");
        JPanel mainpanel = new JPanel();
   
        progressBar = new JProgressBar(0, 100);
        progressBar.setValue(0);
        progressBar.setStringPainted(true);

        font_querry = new Font("serif", Font.PLAIN, 24);
       
        output = new JTextArea(30,100);
        output.setLineWrap(true);
        output.setWrapStyleWord(true);
        output.setEditable(true);
        output.setFont(font_output);
       
        JScrollPane oScroller = new JScrollPane(output);
        oScroller.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
        oScroller.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);

        querry = new JTextArea(2,60);
        querry.setLineWrap(true);
        querry.setWrapStyleWord(true);
        querry.setEditable(true);
        querry.setFont(font_querry);
        querry.setForeground(Color.BLUE);
       
        JScrollPane qScroller = new JScrollPane(querry);
        qScroller.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
        qScroller.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);

        sendbutton = new JButton("RUN QUERRY");

        sendbutton.addActionListener(new SendButtonListener());

        mainpanel.add(qScroller);
        mainpanel.add(oScroller);
        mainpanel.add(sendbutton);
        mainpanel.add(progressBar);

        setUpNetworking();

        Thread readerThread = new Thread(new outputReader());
        readerThread.start();

        frame.getContentPane().add(BorderLayout.CENTER, mainpanel);
        frame.setSize(1200,700);
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLocationRelativeTo(null);
        frame.setResizable(false);
    }

    public static void main(String[] args){

        new HadoopClient().go();
       
    }

    private void setUpNetworking(){

        try{

            sock = new Socket("10.0.3.23", 5000);
            InputStreamReader streamReader = new InputStreamReader(sock.getInputStream());
            reader = new BufferedReader(streamReader);
            writer = new PrintWriter(sock.getOutputStream());
            System.out.println("Networking established!!");

        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public class SendButtonListener implements ActionListener{

        public void actionPerformed(ActionEvent e){
            try{

                output.setText("");
               
                writer.println(querry.getText());
                writer.flush();
               
            }catch(Exception ex){
                ex.printStackTrace();
            }

            querry.setText("");
            querry.requestFocus();
        }
    }

    public class outputReader implements Runnable{

        public void run(){
            String message;
            try{
                while((message = reader.readLine()) != null){
                    System.out.println("read" + message);
                    output.append(message + "\n");
                    //output.setText(message + "\n");
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

}